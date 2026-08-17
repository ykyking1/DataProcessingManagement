//! .tab -> .parquet dönüştürücü (streaming, zstd sıkıştırmalı).
//!
//! `prototypes/tab_to_parquet.py`'nin üretim (Rust) çevirisi -- aynı mantık,
//! aynı çıktı sözleşmesi. Ayrıntılı gerekçeler için docs/plan_dokumani.md.
//!
//! Girdi: .tab dosyası (tab-ayraçlı, ilk satır header, ilk sütun timestamp,
//! geri kalan sütunlar sayısal değerler). docs/plan_dokumani.md Bölüm 3.6'da
//! netleşen detay: her satır sonunda fazladan bir '\t' var -- split'ten önce
//! temizlenmeli, yoksa sütun hizalaması kayar.
//!
//! Çıktı: .parquet dosyası (tüm sayısal sütunlar Float64, zstd sıkıştırma --
//! string DEĞİL, binary numeric -- bkz. plan Bölüm 3.1).
//!
//! Bellek prensibi: --chunk-rows kadar satır okunup bir Arrow RecordBatch'e
//! dönüştürülür, parquet writer'a akış halinde yazılır -- .tab dosyasının
//! tamamı asla RAM'de tutulmaz. ÖNEMLİ: peak belleği asıl sınırlayan
//! --max-row-group-rows'tur, --chunk-rows değil -- parquet writer, kendi
//! row-group'u (varsayılan parquet-rs sınırı: 1.048.576 satır) dolana ya da
//! close() çağrılana kadar yazılan tüm batch'leri bellekte tutar. Bu ikisini
//! karıştırmamak önemli (2026-08-14: ölçümle doğrulandı, bkz.
//! docs/plan_dokumani.md -- --chunk-rows'u tek başına küçültmenin peak
//! belleğe ölçülebilir bir faydası yok).
//!
//! NOT: .tab formatı gerçek exe çıktısıyla henüz uçtan uca doğrulanmadı
//! (bkz. plan Bölüm 5, madde 1) -- gerçek örnek geldiğinde split_tab_line
//! ve sayı parse mantığının gözden geçirilmesi gerekebilir.
//!
//! Kullanım:
//!     tab_to_parquet --input sample.tab --output sample.parquet

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use clap::Parser;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use sha2::{Digest, Sha256};

#[derive(Parser)]
#[command(about = ".tab -> .parquet dönüştürücü (streaming, zstd sıkıştırmalı)")]
struct Args {
    #[arg(long)]
    input: String,
    #[arg(long)]
    output: String,
    #[arg(long, default_value_t = 10_000)]
    chunk_rows: usize,
    #[arg(long, default_value = "zstd")]
    compression: String,
    /// Parquet row-group başına azami satır sayısı. NOT: parquet-rs'nin
    /// varsayılanı 1.048.576 satır -- bizim dosyalarımız (yüz binlerce
    /// satır) bunun altında kaldığı için writer TÜM dosyayı tek row-group
    /// olarak close()'a kadar bellekte tutuyordu (--chunk-rows'un aksine,
    /// bu ayar peak belleği gerçekten sınırlıyor -- 2026-08-14 ölçümüyle
    /// doğrulandı, bkz. docs/plan_dokumani.md).
    #[arg(long, default_value_t = 100_000)]
    max_row_group_rows: usize,
}

struct ConvertResult {
    row_count: u64,
    content_fingerprint: String,
}

fn parse_compression(name: &str) -> Compression {
    match name.to_lowercase().as_str() {
        "zstd" => Compression::ZSTD(ZstdLevel::default()),
        "snappy" => Compression::SNAPPY,
        "gzip" => Compression::GZIP(GzipLevel::default()),
        "none" | "uncompressed" => Compression::UNCOMPRESSED,
        other => panic!("Bilinmeyen sıkıştırma türü: {other} (zstd|snappy|gzip|none)"),
    }
}

/// docs/plan_dokumani.md Bölüm 3.6: her satır sonunda fazladan bir '\t' var.
/// split'ten önce temizlenmezse, beklenen sütun sayısından bir fazla (boş)
/// eleman çıkar ve sütun hizalaması kayar.
fn split_tab_line(line: &str) -> Vec<&str> {
    line.strip_suffix('\t').unwrap_or(line).split('\t').collect()
}

fn convert(
    tab_path: &str,
    parquet_path: &str,
    chunk_rows: usize,
    compression: Compression,
    max_row_group_rows: usize,
) -> ConvertResult {
    let file = File::open(tab_path)
        .unwrap_or_else(|e| panic!("giriş dosyası açılamadı ({tab_path}): {e}"));
    let mut reader = BufReader::new(file);

    let mut header_line = String::new();
    reader
        .read_line(&mut header_line)
        .unwrap_or_else(|e| panic!("header satırı okunamadı: {e}"));
    let header_line = header_line.trim_end_matches(['\n', '\r']);
    let header_fields = split_tab_line(header_line);
    let ts_col_name = header_fields[0].to_string();
    let column_names: Vec<String> = header_fields[1..].iter().map(|s| s.to_string()).collect();
    let num_columns = column_names.len();

    // Tüm sayısal sütunlar Float64 -- string DEĞİL (plan Bölüm 3.1: ClickHouse/
    // Parquet'te string hem daha büyük hem sıralama/filtre açısından yanlış
    // sonuç veriyor).
    let mut fields = vec![Field::new(&ts_col_name, DataType::Float64, false)];
    for name in &column_names {
        fields.push(Field::new(name, DataType::Float64, false));
    }
    let schema = Arc::new(Schema::new(fields));

    let props = WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_size(max_row_group_rows)
        .build();
    let out_file = File::create(parquet_path)
        .unwrap_or_else(|e| panic!("çıktı dosyası oluşturulamadı ({parquet_path}): {e}"));
    let mut writer = ArrowWriter::try_new(out_file, schema.clone(), Some(props))
        .expect("parquet writer oluşturulamadı");

    let mut row_count: u64 = 0;
    let mut col_sum = vec![0f64; num_columns];
    let mut col_min = vec![f64::INFINITY; num_columns];
    let mut col_max = vec![f64::NEG_INFINITY; num_columns];

    let mut buf_ts: Vec<f64> = Vec::with_capacity(chunk_rows);
    let mut buf_vals: Vec<Vec<f64>> = (0..num_columns)
        .map(|_| Vec::with_capacity(chunk_rows))
        .collect();

    let t0 = Instant::now();
    let mut chunk_idx = 0u32;

    macro_rules! flush {
        () => {
            if !buf_ts.is_empty() {
                for (i, col) in buf_vals.iter().enumerate() {
                    let mut s = 0f64;
                    let mut mn = f64::INFINITY;
                    let mut mx = f64::NEG_INFINITY;
                    for &v in col {
                        s += v;
                        if v < mn {
                            mn = v;
                        }
                        if v > mx {
                            mx = v;
                        }
                    }
                    col_sum[i] += s;
                    if mn < col_min[i] {
                        col_min[i] = mn;
                    }
                    if mx > col_max[i] {
                        col_max[i] = mx;
                    }
                }

                // NOT: Vec<f64>'ün sahipliğini Float64Array doğrudan alabiliyor
                // (From<Vec<f64>>) -- .clone() + sonrasında .clear() yapmak
                // gereksiz bir tam kopya demekti (1000 sütunda flush başına
                // ~400MB boşa memcpy). mem::replace ile sahipliği devralıp
                // yerine taze/boş bir Vec bırakıyoruz -- hem kopya yok hem
                // bir sonraki chunk için kapasite hazır (2026-08-15 düzeltmesi,
                // bkz. docs/plan_dokumani.md).
                let n = buf_ts.len();
                let mut arrays: Vec<ArrayRef> = Vec::with_capacity(num_columns + 1);
                let ts_owned = std::mem::replace(&mut buf_ts, Vec::with_capacity(chunk_rows));
                arrays.push(Arc::new(Float64Array::from(ts_owned)));
                for col in buf_vals.iter_mut() {
                    let owned = std::mem::replace(col, Vec::with_capacity(chunk_rows));
                    arrays.push(Arc::new(Float64Array::from(owned)));
                }
                let batch = RecordBatch::try_new(schema.clone(), arrays)
                    .expect("record batch oluşturulamadı");
                writer.write(&batch).expect("parquet'e yazılamadı");

                row_count += n as u64;
                chunk_idx += 1;
                let elapsed = t0.elapsed().as_secs_f64();
                let rate = if elapsed > 0.0 {
                    row_count as f64 / elapsed
                } else {
                    0.0
                };
                println!(
                    "  Parça {chunk_idx}: {n} satır (toplam {row_count}, {rate:.0} satır/sn)"
                );
            }
        };
    }

    for (line_no, line) in reader.lines().enumerate() {
        let line = line.unwrap_or_else(|e| panic!("satır okunamadı (satır {}): {e}", line_no + 2));
        let fields = split_tab_line(&line);
        if fields.len() != num_columns + 1 {
            panic!(
                "sütun sayısı uyuşmazlığı (satır {}): beklenen {} bulunan {}",
                line_no + 2,
                num_columns + 1,
                fields.len()
            );
        }
        let ts: f64 = fields[0]
            .parse()
            .unwrap_or_else(|e| panic!("timestamp parse hatası (satır {}): {e}", line_no + 2));
        buf_ts.push(ts);
        for (i, f) in fields[1..].iter().enumerate() {
            let v: f64 = f.parse().unwrap_or_else(|e| {
                panic!("sayısal değer parse hatası (satır {}, sütun {}): {e}", line_no + 2, i + 1)
            });
            buf_vals[i].push(v);
        }
        if buf_ts.len() >= chunk_rows {
            flush!();
        }
    }
    flush!();
    writer.close().expect("parquet writer kapatılamadı");

    // İçerik parmak izi -- Postgres manifest'teki content_fingerprint alanına
    // yazılacak (plan Bölüm 3.5/3.7). NOT: küçük FP birikim farklarını absorbe
    // etmek için 4 ondalık basamağa yuvarlanıyor -- exact hash yerine tolerans
    // temelli karşılaştırma önerilir. Rust'ın round-half-away-from-zero'su ile
    // Python'ın round-half-to-even'ı nadir durumlarda 1 birim farklı
    // yuvarlayabilir -- bu fingerprint'in Python prototipiyle bit-bit
    // eşleşmesi garanti değildir, sadece Rust içi tutarlı bir özet.
    let rounded_sums: Vec<f64> = col_sum
        .iter()
        .map(|v| (v * 10_000.0).round() / 10_000.0)
        .collect();
    let fingerprint_input = format!(
        "{{\"col_sum\": [{}], \"row_count\": {}}}",
        rounded_sums
            .iter()
            .map(|v| format_float(*v))
            .collect::<Vec<_>>()
            .join(", "),
        row_count
    );
    let mut hasher = Sha256::new();
    hasher.update(fingerprint_input.as_bytes());
    let content_fingerprint = format!("{:x}", hasher.finalize());

    // col_min/col_max şu an fingerprint'e dahil değil (Python prototipiyle aynı
    // davranış) -- ileride verify_conversion.py mantığının Rust'a taşınması
    // durumunda toleranslı karşılaştırma için kullanılabilir.
    let _ = (&col_min, &col_max);

    ConvertResult {
        row_count,
        content_fingerprint,
    }
}

/// Python'ın `repr(float)` / `json.dumps` float çıktısına yakın biçim
/// (tam sayı değerler için ".0" ekler, aksi halde kısa/round-trip gösterim).
fn format_float(v: f64) -> String {
    if v.is_finite() && v == v.trunc() && v.abs() < 1e16 {
        format!("{v:.1}")
    } else {
        format!("{v}")
    }
}

fn main() {
    let args = Args::parse();
    let compression = parse_compression(&args.compression);
    let result = convert(
        &args.input,
        &args.output,
        args.chunk_rows,
        compression,
        args.max_row_group_rows,
    );

    let tab_size = std::fs::metadata(&args.input).map(|m| m.len()).unwrap_or(0);
    let parquet_size = std::fs::metadata(&args.output).map(|m| m.len()).unwrap_or(0);

    println!();
    println!("Tamamlandı: {} satır", result.row_count);
    println!(
        ".tab boyutu:     {:.1} MB",
        tab_size as f64 / (1024.0 * 1024.0)
    );
    println!(
        ".parquet boyutu: {:.1} MB",
        parquet_size as f64 / (1024.0 * 1024.0)
    );
    if parquet_size > 0 {
        println!(
            "Sıkıştırma oranı: {:.2}x küçülme",
            tab_size as f64 / parquet_size as f64
        );
    }
    println!(
        "İçerik parmak izi (Postgres manifest için): {}...",
        &result.content_fingerprint[..16]
    );
}
