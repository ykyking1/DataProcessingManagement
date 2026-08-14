//! Sentetik .tab test dosyası üretici (ölçek/benchmark testleri için).
//!
//! docs/plan_dokumani.md Bölüm 3.6'daki gerçek .tab formatına uygun
//! (tab-ayraçlı, her satır sonunda fazladan '\t', ilk sütun timestamp)
//! rastgele veri üretir -- streaming dönüştürücünün sabit bellekle
//! çalıştığını büyük dosyalarla doğrulamak/ölçmek için.
//!
//! NOT: Bu, gerçek .ham/.tab formatının varsayımsal bir stand-in'i --
//! gerçek format netleşince (plan Bölüm 5, madde 1) burası da
//! güncellenmesi gerekebilir. Şu an sadece sentetik/test amaçlı.
//!
//! Kullanım:
//!     generate_test_tab --output big.tab --target-size-gb 10 --columns 300

use std::fmt::Write as FmtWrite;
use std::fs::File;
use std::io::{BufWriter, Write as IoWrite};
use std::time::Instant;

use clap::Parser;

#[derive(Parser)]
#[command(about = "Sentetik .tab test dosyası üretici")]
struct Args {
    #[arg(long)]
    output: String,
    #[arg(long, default_value_t = 10.0)]
    target_size_gb: f64,
    #[arg(long, default_value_t = 300)]
    columns: usize,
    /// sütun değerleri -range..range arasında üretilir
    #[arg(long, default_value_t = 500.0)]
    range: f64,
    /// timestamp adımı (saniye) -- plan'daki "saniyeden kısa periyot" varsayımı
    #[arg(long, default_value_t = 0.004)]
    dt: f64,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// kaç satırda bir ilerleme yazdırılsın
    #[arg(long, default_value_t = 200_000)]
    progress_every: u64,
}

/// Bağımlılık eklememek için basit/hızlı bir xorshift64* PRNG -- kriptografik
/// kalite gerekmiyor, sadece istatistiksel olarak makul dağılımlı sentetik
/// test verisi.
struct Xorshift64(u64);

impl Xorshift64 {
    fn new(seed: u64) -> Self {
        // 0 durumu xorshift'i kilitler, seed'i garanti sıfır-olmayan yap.
        Xorshift64(if seed == 0 { 0x9E3779B97F4A7C15 } else { seed })
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    /// [lo, hi) aralığında yaklaşık uniform bir f64 (53 bit mantissa hassasiyeti).
    fn range_f64(&mut self, lo: f64, hi: f64) -> f64 {
        let frac = (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64);
        lo + frac * (hi - lo)
    }
}

fn main() {
    let args = Args::parse();
    let target_bytes = (args.target_size_gb * 1024.0 * 1024.0 * 1024.0) as u64;

    let file = File::create(&args.output)
        .unwrap_or_else(|e| panic!("çıktı dosyası oluşturulamadı ({}): {e}", args.output));
    let mut w = BufWriter::with_capacity(8 * 1024 * 1024, file);
    let mut rng = Xorshift64::new(args.seed);

    // Header -- plan Bölüm 3.6: gerçek satırlar gibi sonunda fazladan '\t' var.
    write!(w, "timestamp").unwrap();
    for i in 0..args.columns {
        write!(w, "\tcol{i}").unwrap();
    }
    write!(w, "\t\n").unwrap();

    let mut bytes_written: u64 = 0;
    let mut ts = 0.0f64;
    let mut row_count: u64 = 0;
    let t0 = Instant::now();
    let mut line_buf = String::with_capacity(args.columns * 20 + 32);

    while bytes_written < target_bytes {
        line_buf.clear();
        write!(line_buf, "{ts:.6}").unwrap();
        for _ in 0..args.columns {
            let v = rng.range_f64(-args.range, args.range);
            write!(line_buf, "\t{v:.6}").unwrap();
        }
        line_buf.push_str("\t\n");

        w.write_all(line_buf.as_bytes()).expect("yazma hatası");
        bytes_written += line_buf.len() as u64;
        row_count += 1;
        ts += args.dt;

        if row_count % args.progress_every == 0 {
            let elapsed = t0.elapsed().as_secs_f64();
            let mb = bytes_written as f64 / (1024.0 * 1024.0);
            println!(
                "  {row_count} satır, {mb:.0} MB yazıldı ({:.1} MB/sn)",
                mb / elapsed.max(0.001)
            );
        }
    }
    w.flush().expect("flush hatası");

    let elapsed = t0.elapsed().as_secs_f64();
    let gb = bytes_written as f64 / (1024.0 * 1024.0 * 1024.0);
    println!();
    println!(
        "Tamamlandı: {row_count} satır, {gb:.2} GB, {elapsed:.1} sn ({:.1} MB/sn)",
        (bytes_written as f64 / (1024.0 * 1024.0)) / elapsed.max(0.001)
    );
    println!("Sütun sayısı: {}", args.columns);
    println!("Çıktı: {}", args.output);
}
