# Fluent Bit Kinesis Firehose Simple Aggregation 実装計画

## 問題の概要

**Issue**: [https://github.com/fluent/fluent-bit/issues/9279](https://github.com/fluent/fluent-bit/issues/9279)

現在のFluent Bitの`kinesis_firehose`プラグインは、各ログレコードを個別のFirehoseレコードとして送信しているため、コストが高くなっている。AWS公式のFirehoseプラグインが持つ`simple_aggregation`機能を実装することで、複数のログレコードを1つのFirehoseレコードに集約し、API呼び出し数を削減してコストを下げる必要がある。

## 現在の実装分析

### 現在の処理フロー

1. **データ処理**: `process_and_send_records()` → 各ログイベントを個別に処理
2. **イベント追加**: `add_event()` → 各イベントを別々のFirehoseレコードとして格納
3. **送信**: `send_log_events()` → PutRecordBatchで最大500レコードを送信

### 現在の制限事項

- 各ログレコードが1つのFirehoseレコードになる
- 多数の小さなレコードがある場合、API呼び出し数が多くなる
- AWS料金はレコード数に比例するため、コストが高い

## Simple Aggregation機能の仕様

### 機能概要

- 複数のログレコードを1つのFirehoseレコードに結合
- レコード間は改行文字(`\n`)で区切る
- 最大レコードサイズ: 1 MB (base64エンコード前)
- 設定可能なオプションとして提供

### AWS Firehose API制限

- 1回のPutRecordBatch: 最大500レコード
- 1レコードの最大サイズ: 1,000 KB (base64エンコード前)
- リクエスト全体の最大サイズ: 4 MB

## 実装設計

### 1. 設定パラメータの追加

```c
struct flb_firehose {
    // ... 既存のフィールド
    int simple_aggregation;  // 機能の有効/無効
    size_t max_record_size;  // 集約レコードの最大サイズ (デフォルト: 900KB)
};
```

**設定マップに追加**:
```c
{
    FLB_CONFIG_MAP_BOOL, "simple_aggregation", "false",
    0, FLB_TRUE, offsetof(struct flb_firehose, simple_aggregation),
    "Enable simple aggregation to combine multiple log records into a single Firehose record"
},
{
    FLB_CONFIG_MAP_SIZE, "max_aggregated_record_size", "900KB",
    0, FLB_TRUE, offsetof(struct flb_firehose, max_record_size),
    "Maximum size for aggregated records (max 1000KB)"
}
```

### 2. データ構造の拡張

```c
struct aggregated_record {
    char *data;              // 集約されたJSONデータ
    size_t size;             // 現在のサイズ
    size_t capacity;         // バッファ容量
    int log_count;           // 含まれるログレコード数
    struct timespec first_timestamp;  // 最初のレコードのタイムスタンプ
};

struct flush {
    // ... 既存のフィールド
    struct aggregated_record current_aggregated;  // 現在構築中の集約レコード
};
```

### 3. 集約ロジックの実装

#### a) `process_aggregated_event()` 関数の追加

```c
static int process_aggregated_event(struct flb_firehose *ctx, 
                                   struct flush *buf,
                                   const char *json_data, 
                                   size_t json_len,
                                   struct flb_time *tms)
{
    struct aggregated_record *agg = &buf->current_aggregated;
    size_t needed_size = json_len + 1; // +1 for newline
    
    // 初回または容量不足時の初期化
    if (agg->data == NULL || (agg->size + needed_size) > ctx->max_record_size) {
        // 現在の集約レコードを送信
        if (agg->size > 0) {
            int ret = finalize_aggregated_record(ctx, buf);
            if (ret < 0) return ret;
        }
        
        // 新しい集約レコードを開始
        ret = init_aggregated_record(ctx, buf, tms);
        if (ret < 0) return ret;
    }
    
    // JSONデータを集約レコードに追加
    if (agg->size > 0) {
        // 既存データがある場合は改行を追加
        agg->data[agg->size] = '\n';
        agg->size++;
    }
    
    memcpy(agg->data + agg->size, json_data, json_len);
    agg->size += json_len;
    agg->log_count++;
    
    return 0;
}
```

#### b) `finalize_aggregated_record()` 関数の追加

```c
static int finalize_aggregated_record(struct flb_firehose *ctx, struct flush *buf)
{
    struct aggregated_record *agg = &buf->current_aggregated;
    struct firehose_event *event;
    size_t b64_len;
    int ret;
    
    if (agg->size == 0) return 0;
    
    // Base64エンコード
    size_t b64_capacity = (agg->size * 1.5) + 4;
    char *b64_data = flb_malloc(b64_capacity);
    if (!b64_data) return -1;
    
    ret = flb_base64_encode((unsigned char *)b64_data, b64_capacity, &b64_len,
                           (unsigned char *)agg->data, agg->size);
    if (ret != 0) {
        flb_free(b64_data);
        return -1;
    }
    
    // イベントとして登録
    event = &buf->events[buf->event_index];
    event->json = b64_data;
    event->len = b64_len;
    event->timestamp = agg->first_timestamp;
    
    buf->event_index++;
    buf->data_size += b64_len + PUT_RECORD_BATCH_PER_RECORD_LEN;
    
    // 集約レコードをリセット
    agg->size = 0;
    agg->log_count = 0;
    
    return 0;
}
```

### 4. メイン処理の修正

#### `process_and_send_records()`の修正

```c
int process_and_send_records(struct flb_firehose *ctx, struct flush *buf,
                             const char *data, size_t bytes)
{
    // ... 既存の初期化コード
    
    while ((ret = flb_log_event_decoder_next(&log_decoder, &log_event)) == FLB_EVENT_DECODER_SUCCESS) {
        // JSONシリアライゼーション
        ret = flb_msgpack_to_json(json_buf, json_buf_size, log_event.body);
        if (ret <= 0) continue;
        
        if (ctx->simple_aggregation) {
            // 集約モード
            ret = process_aggregated_event(ctx, buf, json_buf, ret, &log_event.timestamp);
        } else {
            // 従来モード (既存のadd_event呼び出し)
            ret = add_event(ctx, buf, log_event.body, &log_event.timestamp);
        }
        
        if (ret < 0) goto error;
        i++;
    }
    
    // 残りの集約レコードを送信
    if (ctx->simple_aggregation) {
        ret = finalize_aggregated_record(ctx, buf);
        if (ret < 0) return -1;
    }
    
    // ... 残りの処理
}
```

### 5. メモリ管理

#### リソース管理の追加

```c
static void cleanup_aggregated_record(struct aggregated_record *agg)
{
    if (agg->data) {
        flb_free(agg->data);
        agg->data = NULL;
    }
    agg->size = 0;
    agg->capacity = 0;
    agg->log_count = 0;
}

// flush_destroy()に追加
void flush_destroy(struct flush *buf)
{
    if (buf) {
        cleanup_aggregated_record(&buf->current_aggregated);
        // ... 既存のクリーンアップ
    }
}
```

## 実装上の考慮事項

### 1. パフォーマンス最適化

- **バッファの事前割り当て**: 初期容量を十分に確保（例：64KB）
- **リアロケーションの最小化**: 指数的にサイズを増加
- **メモリコピーの最適化**: 可能な限り一度のコピーで済ませる

### 2. エラーハンドリング

- **部分的な失敗**: 集約中にエラーが発生した場合の復旧機能
- **メモリ不足**: 適切なエラーメッセージとリソースクリーンアップ
- **サイズ制限超過**: 自動的に新しいレコードを開始

### 3. 後方互換性

- デフォルトでは無効（`simple_aggregation = false`）
- 既存の設定に影響を与えない
- 段階的な移行が可能

### 4. ログとデバッグ

```c
flb_plg_debug(ctx->ins, "Aggregated %d log records into single Firehose record (size: %zu bytes)", 
              agg->log_count, agg->size);
```

## テスト計画

### 1. 単体テスト

- 集約ロジックの正確性
- サイズ制限の検証
- エラーケースのハンドリング

### 2. 統合テスト

- 実際のFirehose送信テスト
- 大量データでのパフォーマンステスト
- 設定の組み合わせテスト

### 3. 回帰テスト

- 既存機能への影響確認
- 設定の後方互換性確認

## 期待される効果

### コスト削減

- **集約前**: 1000レコード = 1000 API呼び出し
- **集約後**: 1000レコード = 10-50 API呼び出し（集約率により変動）
- **コスト削減率**: 最大95%の削減が可能

### パフォーマンス向上

- API呼び出し回数の削減
- ネットワーク帯域幅の効率的利用
- レイテンシの改善

## 実装フェーズ

### Phase 1: 基本実装
- 設定パラメータの追加
- 基本的な集約ロジック
- 簡単なテストケース

### Phase 2: 最適化
- パフォーマンスチューニング
- エラーハンドリングの強化
- 包括的なテストスイート

### Phase 3: 文書化とリリース
- ユーザードキュメントの作成
- 移行ガイドの作成
- プロダクション検証

## 関連ファイル

- `plugins/out_kinesis_firehose/firehose.h` - 構造体定義の拡張
- `plugins/out_kinesis_firehose/firehose.c` - 設定処理の修正
- `plugins/out_kinesis_firehose/firehose_api.c` - メインロジックの実装
- `tests/runtime/out_firehose.c` - テストケースの追加

この実装により、Fluent BitのKinesis Firehoseプラグインは、AWS公式プラグインと同等のコスト効率性を実現できます。