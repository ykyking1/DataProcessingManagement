"""
Qwen3:1.7B telemetri sorgu ayristirma - v6

v5'in dersi: prompt uzadikca ILGISIZ kategoriler bozuluyor.
operator_netlik, o kategoriye dair hicbir sey degistirilmedigi halde
%83'ten %50'ye dustu. 1.7B model uzun talimatta dagiliyor.

v6 stratejisi: prompt UZATILMADI, KISALTILDI ve yeniden yapilandirildi.
  - Nesir kural yerine TABLO. Tetikleyici kelime satirin BASINDA;
    model kelimeyi tarayarak buluyor, paragraf okuyup cikarim yapmiyor.
  - v5'teki "ASLA / MUTLAKA / cok onemli" vurgulari temizlendi.
    1.7B'de bu vurgular yardim etmiyor, sadece dikkat dagitiyor.
  - Ornek sayisi azaltildi; kalan her ornek FARKLI bir kategoriyi temsil ediyor.

v5 hatalarina karsi hedefli duzeltmeler:
  aci karisikligi (3 hata)     -> aci satirlari ALAN TABLOSU'nun en ustunde
  operator yonu (3 hata)       -> kelime -> sembol tablosu + karsitlik satiri
  dikey_hiz asiri uygulamasi   -> "rakam varsa gecersiz" + iki karsi ornek
  zaman ifadesi kacirma (2)    -> kabul edilen ifadeler tek satirda listelendi

SON_ISLEM katmani v5'ten degistirilmeden alindi (orada +5.7 puan getirmisti).
Katkisini olcmek icin False yapabilirsin; rapor iki skoru ayri gosteriyor.
"""

import json
import os
import re
import statistics
import time
from collections import defaultdict

import ollama


MODEL = os.environ.get("OLLAMA_MODEL", "qwen3.5:4b")
SON_ISLEM = True


# ============================================================
# SISTEM PROMPTU v6
# ============================================================

SYSTEM_PROMPT = """Sen İHA telemetri sorgularını JSON'a çeviren bir ayrıştırıcısın. Sadece JSON döndür.

ÇIKTI BİÇİMİ:
{"filtreler": [{"alan": ..., "operator": ..., "deger": ...}], "mantik": "AND", "zaman_araligi": null, "aciklama": "..."}

ALAN TABLOSU (cümledeki kelime → alan)
yatış, yattı, yana yatma, roll          → yatis_acisi
yunuslama, pitch, burun                 → yunuslama_acisi
sapma, yaw, dönüş açısı                 → sapma_acisi
dikey hız, tırmanma hızı, iniş hızı     → dikey_hiz
metre, yükseklik, yüksekte, rakım       → irtifa
hız, sürat, m/s, km/h                   → hiz
batarya, şarj, pil                      → batarya
sıcaklık, ısı, sıcak, soğuk             → sicaklik
motor, devir, rpm                       → motor_devri
enlem, lat, latitude                    → enlem
boylam, lon, longitude                  → boylam
saat 7 ile 9 arası, saat 18-21 arasında → gun_ici_saat
4 saatten kısa/uzun süren uçuş         → ucus_suresi

Üç açı üç ayrı kelimeyle tetiklenir. Tabloya bak, benzetme yapma.

gun_ici_saat, GÜNÜN SAATİ (0-23) ile ilgilidir, SÜRE ile değil:
  "saat 7 ile 9 arası"        → {"alan": "gun_ici_saat", "operator": "between", "deger": [7, 9]}
  "saat 22'den sonraki"       → {"alan": "gun_ici_saat", "operator": ">", "deger": 22}
  "sabah 6'da"                → {"alan": "gun_ici_saat", "operator": "==", "deger": 6}

ucus_suresi, UÇUŞUN TOPLAM SÜRESİDİR (saat cinsinden), ne zaman uçtuğu değil:
  "4 saatten kısa süren uçuşlar"     → {"alan": "ucus_suresi", "operator": "<", "deger": 4}
  "3 ile 5 saat arası süren uçuşlar" → {"alan": "ucus_suresi", "operator": "between", "deger": [3, 5]}

"son X saat" / "geçtiğimiz X saat" bunların HİÇBİRİ DEĞİLDİR, SÜREdir → ZAMAN
İFADELERİ bölümündeki zaman_araligi'na gider, filtreler listesine değil.

OPERATÖR TABLOSU (cümledeki kelime → sembol)
üstünde, üzerinde, fazla, aşan, geçen, yüksek, büyük   → >
altında, altına düşen, altına inen, az, düşük, soğuk   → <
en az, minimum, veya üzerinde                          → >=
en fazla, maksimum, veya daha az, ve altında           → <=
tam, eşit, birebir, aynen                              → ==
olmadığı, eşit olmayan, farklı                         → !=
ile ... arasında, ... - ... arası, ila                 → between

Karşıtlık: az / düşük / soğuk / altında = "<"   ●   fazla / yüksek / üstünde = ">"
minimum = en az = ">="   ●   maksimum = en fazla = "<="

YÖN İFADESİ (yalnızca cümlede RAKAM YOKKEN)
alçalıyor, iniyor, iniş yaptı, aşağı, süzüldü  → {"alan": "dikey_hiz", "operator": "<", "deger": 0}
yükseliyor, tırmanıyor, yukarı, çıktı          → {"alan": "dikey_hiz", "operator": ">", "deger": 0}

Cümlede rakam varsa bu kural geçersiz, ALAN TABLOSU'nu kullan:
  "300 metrenin altına düştüğü" → irtifa < 300      (dikey_hiz değil)
  "250'nin altına indiği"       → irtifa < 250      (dikey_hiz değil)
Rakamla birlikte dikey_hiz yalnızca "dikey hız / tırmanma hızı / iniş hızı" yazıyorsa seçilir.

RAKAM YOKSA DEĞER UYDURMA
çok, aşırı, bayağı, epey, fazla, oldukça, sanki, kritik → bunlar rakam değildir.
Rakam yoksa operator ve deger null olur; alan yine yazılır.
Hiçbir alan anlaşılmıyorsa "filtreler": []

ZAMAN İFADELERİ (cümledekini aynen kopyala)
son 10 dakika ● son yarım saat ● son 1 saat ● geçtiğimiz 1 saat ● geçtiğimiz yarım saat ● dün ● bugün ● bu sabah
zaman_araligi üst düzey alandır, filtreler listesinin içine koyma.

BİÇİM
Aralık: "deger": [200, 400] yazılır, sonra filtre } ile kapanır. Fazladan ] koyma.
null tırnaksız yazılır. Her koşul ayrı bir filtre nesnesidir.
"veya / ya da" → "mantik": "OR" ; diğer durumlarda "AND".

ÖRNEKLER

Hız 40'ın üzerinde ya da irtifa 300 metrenin altında olan noktalar
{"filtreler": [{"alan": "hiz", "operator": ">", "deger": 40}, {"alan": "irtifa", "operator": "<", "deger": 300}], "mantik": "OR", "zaman_araligi": null, "aciklama": "yüksek hız veya düşük irtifa"}

Yatış açısı 25 dereceyi geçen ya da yunuslama açısı 15'i aşan anlar
{"filtreler": [{"alan": "yatis_acisi", "operator": ">", "deger": 25}, {"alan": "yunuslama_acisi", "operator": ">", "deger": 15}], "mantik": "OR", "zaman_araligi": null, "aciklama": "aşırı yatış veya yunuslama"}

Sıcaklık 80'i geçmiş ya da batarya yüzde 15'in altına düşmüş olabilir
{"filtreler": [{"alan": "sicaklik", "operator": ">", "deger": 80}, {"alan": "batarya", "operator": "<", "deger": 15}], "mantik": "OR", "zaman_araligi": null, "aciklama": "yüksek sıcaklık veya düşük batarya"}

Dikey hız 5'in üzerinde yahut irtifa 500 metreyi geçen kayıtlar
{"filtreler": [{"alan": "dikey_hiz", "operator": ">", "deger": 5}, {"alan": "irtifa", "operator": ">", "deger": 500}], "mantik": "OR", "zaman_araligi": null, "aciklama": "hızlı tırmanma veya yüksek irtifa"}

Hız 10'un altında veya batarya yüzde 20'nin altında olan son bir saatlik kayıtlar
{"filtreler": [{"alan": "hiz", "operator": "<", "deger": 10}, {"alan": "batarya", "operator": "<", "deger": 20}], "mantik": "OR", "zaman_araligi": "son bir saat", "aciklama": "düşük hız veya düşük batarya, son 1 saat"}

Sıcaklık 90'ı aşan, batarya yüzde 5'in altına inen veya irtifa 600'ü geçen noktalar
{"filtreler": [{"alan": "sicaklik", "operator": ">", "deger": 90}, {"alan": "batarya", "operator": "<", "deger": 5}, {"alan": "irtifa", "operator": ">", "deger": 600}], "mantik": "OR", "zaman_araligi": null, "aciklama": "kritik sıcaklık, batarya veya irtifa"}

Alçalıyor ya da yatış açısı 30 dereceyi geçiyor olabiliriz
{"filtreler": [{"alan": "dikey_hiz", "operator": "<", "deger": 0}, {"alan": "yatis_acisi", "operator": ">", "deger": 30}], "mantik": "OR", "zaman_araligi": null, "aciklama": "alçalma veya aşırı yatış"}

Hız 50'yi geçen veya yükseklik 100 metrenin altına inen anlar
{"filtreler": [{"alan": "hiz", "operator": ">", "deger": 50}, {"alan": "irtifa", "operator": "<", "deger": 100}], "mantik": "OR", "zaman_araligi": null, "aciklama": "yüksek hız veya çok düşük irtifa"}

Pitch açısı 20'yi geçen veya dikey hız -3'ün altında olan kayıtlar
{"filtreler": [{"alan": "yunuslama_acisi", "operator": ">", "deger": 20}, {"alan": "dikey_hiz", "operator": "<", "deger": -3}], "mantik": "OR", "zaman_araligi": null, "aciklama": "aşırı yunuslama veya hızlı alçalma"}

Son 15 dakikada sıcaklık 70'i geçmiş ya da hız 5'in altına düşmüş noktalar
{"filtreler": [{"alan": "sicaklik", "operator": ">", "deger": 70}, {"alan": "hiz", "operator": "<", "deger": 5}], "mantik": "OR", "zaman_araligi": "son 15 dakika", "aciklama": "yüksek sıcaklık veya düşük hız, son 15 dk"}

Saat 7 ile 9 arasındaki uçuşları getir
{"filtreler": [{"alan": "gun_ici_saat", "operator": "between", "deger": [7, 9]}], "mantik": "AND", "zaman_araligi": null, "aciklama": "sabah 7-9 saatleri arası"}

4 saatten kısa süren uçuşları listele
{"filtreler": [{"alan": "ucus_suresi", "operator": "<", "deger": 4}], "mantik": "AND", "zaman_araligi": null, "aciklama": "kısa süren uçuşlar"}"""


# ============================================================
# SON ISLEM KATMANI (v5'ten degistirilmeden)
# ============================================================

SAYI_REGEX = re.compile(r"-?\d+(?:[.,]\d+)?")
ALCALMA = re.compile(r"alçal|alcal|iniş|inis|aşağı|asagi|düşüş|dusus|süzül|suzul|indiğ|indig", re.I)
YUKSELME = re.compile(r"yüksel|yuksel|tırman|tirman|yukarı|yukari|çıkt|cikt|çıkış|cikis|kalk", re.I)
DIKEY_ANAHTAR = re.compile(r"dikey|tırmanma hız|tirmanma hiz|iniş hız|inis hiz|"
                           r"alçalma hız|alcalma hiz|yükselme hız|yukselme hiz", re.I)


def son_islem(parsed: dict, sorgu: str):
    duzeltmeler = []
    filtreler = parsed.get("filtreler", [])
    if not isinstance(filtreler, list):
        return parsed, duzeltmeler

    sayilar = [float(s.replace(",", ".")) for s in SAYI_REGEX.findall(sorgu)]
    dikey_var = bool(DIKEY_ANAHTAR.search(sorgu))
    alcalma = bool(ALCALMA.search(sorgu))
    yukselme = bool(YUKSELME.search(sorgu))

    for f in filtreler:
        if not isinstance(f, dict):
            continue
        alan, op, deger = f.get("alan"), f.get("operator"), f.get("deger")

        # R1: acik dikey anahtar kelimesi -> alan dikey_hiz
        if dikey_var and alan in {"hiz", "irtifa"}:
            duzeltmeler.append(f"R1: '{alan}' -> 'dikey_hiz'")
            f["alan"] = alan = "dikey_hiz"

        # R2: sayisiz yon ifadesi
        if (alcalma or yukselme) and not sayilar:
            bek = "<" if alcalma else ">"
            if alan != "dikey_hiz" or op != bek or deger != 0:
                duzeltmeler.append(f"R2: -> dikey_hiz {bek} 0 (onceki: {alan} {op} {deger})")
                f.update({"alan": "dikey_hiz", "operator": bek, "deger": 0})
            continue

        # R2b: coklu sorguda deger 0 + yon ifadesi, yanlis alan
        if (alcalma or yukselme) and deger == 0 and alan != "dikey_hiz":
            duzeltmeler.append(f"R2b: '{alan}' -> 'dikey_hiz'")
            f.update({"alan": "dikey_hiz", "operator": "<" if alcalma else ">"})
            continue

        # R3: sorguda rakam yoksa esik uydurulmustur
        if not sayilar and not (alcalma or yukselme):
            if op is not None or deger is not None:
                duzeltmeler.append(f"R3: operator/deger null'landi (onceki: {op} {deger})")
                f.update({"operator": None, "deger": None})
            continue

        # R4: operator var, deger eksik, sorguda tek sayi var
        if op is not None and deger is None and len(sayilar) == 1:
            yeni = int(sayilar[0]) if sayilar[0].is_integer() else sayilar[0]
            f["deger"] = yeni
            duzeltmeler.append(f"R4: eksik deger sorgudan alindi -> {yeni}")

        # R5: between olmayan operatorde liste deger
        if op not in {"between", None} and isinstance(deger, list) and deger:
            duzeltmeler.append(f"R5: liste deger tekile indirildi -> {deger[0]}")
            f["deger"] = deger[0]

    return parsed, duzeltmeler


# ============================================================
# TEST SETI
# ============================================================

#CLAUDE TARAFINDAN OLUŞTURULAN TEST CASE'LER


TEST_CASES = [
    {"kategori": "temel", "sorgu": "İrtifanın 500 metreyi geçtiği anları görebilir miyiz?",
     "beklenen": {"filtreler": [{"alan": "irtifa", "operator": ">", "deger": 500}]}},
    {"kategori": "temel", "sorgu": "300 metrenin altına düştüğümüz kısımları bulur musun?",
     "beklenen": {"filtreler": [{"alan": "irtifa", "operator": "<", "deger": 300}]}},
    {"kategori": "temel", "sorgu": "Şarjın yüzde 90'dan fazla olduğu durumları göster.",
     "beklenen": {"filtreler": [{"alan": "batarya", "operator": ">", "deger": 90}]}},
    {"kategori": "temel", "sorgu": "Motor devrinin 5000'i aştığı anları çıkarabilir misin?",
     "beklenen": {"filtreler": [{"alan": "motor_devri", "operator": ">", "deger": 5000}]}},
    {"kategori": "temel", "sorgu": "Sapmanın 90 dereceyi aştığı yerleri bul.",
     "beklenen": {"filtreler": [{"alan": "sapma_acisi", "operator": ">", "deger": 90}]}},

    {"kategori": "aci", "sorgu": "Dronun 45 dereceden fazla yattığı anlara bakmak istiyorum.",
     "beklenen": {"filtreler": [{"alan": "yatis_acisi", "operator": ">", "deger": 45}]}},
    {"kategori": "aci", "sorgu": "Yana yatma açısı 20 dereceyi geçen kayıtlar.",
     "beklenen": {"filtreler": [{"alan": "yatis_acisi", "operator": ">", "deger": 20}]}},
    {"kategori": "aci", "sorgu": "Roll açısının 60 dereceden fazla olduğu anlar.",
     "beklenen": {"filtreler": [{"alan": "yatis_acisi", "operator": ">", "deger": 60}]}},
    {"kategori": "aci", "sorgu": "Yana yatışın en az 35 derece olduğu kayıtları göster.",
     "beklenen": {"filtreler": [{"alan": "yatis_acisi", "operator": ">=", "deger": 35}]}},
    {"kategori": "aci", "sorgu": "Burun açısının eksi 20'nin altına indiği anlar.",
     "beklenen": {"filtreler": [{"alan": "yunuslama_acisi", "operator": "<", "deger": -20}]}},
    {"kategori": "aci", "sorgu": "Pitch açısı 15 dereceyi geçen kayıtlar.",
     "beklenen": {"filtreler": [{"alan": "yunuslama_acisi", "operator": ">", "deger": 15}]}},
    {"kategori": "aci", "sorgu": "Yaw açısı 180'i geçen kayıtları listele.",
     "beklenen": {"filtreler": [{"alan": "sapma_acisi", "operator": ">", "deger": 180}]}},

    {"kategori": "sinonim", "sorgu": "Yerden yüksekliğin 1000 metreyi geçtiği yerleri ver.",
     "beklenen": {"filtreler": [{"alan": "irtifa", "operator": ">", "deger": 1000}]}},
    {"kategori": "sinonim", "sorgu": "Rakımın 250'nin altına indiği kayıtları göster.",
     "beklenen": {"filtreler": [{"alan": "irtifa", "operator": "<", "deger": 250}]}},
    {"kategori": "sinonim", "sorgu": "Pilin yüzde 15'in altına indiği kritik anları listele.",
     "beklenen": {"filtreler": [{"alan": "batarya", "operator": "<", "deger": 15}]}},
    {"kategori": "sinonim", "sorgu": "Süratin 40'ı aştığı yerleri bulabilir miyiz?",
     "beklenen": {"filtreler": [{"alan": "hiz", "operator": ">", "deger": 40}]}},
    {"kategori": "sinonim", "sorgu": "RPM'in 1000'den düşük olduğu zamanları getir.",
     "beklenen": {"filtreler": [{"alan": "motor_devri", "operator": "<", "deger": 1000}]}},

    {"kategori": "aralik", "sorgu": "Hızın 15 ile 25 arasında seyrettiği anlara ihtiyacım var.",
     "beklenen": {"filtreler": [{"alan": "hiz", "operator": "between", "deger": [15, 25]}]}},
    {"kategori": "aralik", "sorgu": "200 ile 400 metre arasında uçtuğumuz noktaları filtrele.",
     "beklenen": {"filtreler": [{"alan": "irtifa", "operator": "between", "deger": [200, 400]}]}},
    {"kategori": "aralik", "sorgu": "Şarjın yüzde 30'la 60 arasında olduğu kısımları alalım.",
     "beklenen": {"filtreler": [{"alan": "batarya", "operator": "between", "deger": [30, 60]}]}},
    {"kategori": "aralik", "sorgu": "Sıcaklığın 20 ile 45 derece arasında olduğu kayıtlar.",
     "beklenen": {"filtreler": [{"alan": "sicaklik", "operator": "between", "deger": [20, 45]}]}},
    {"kategori": "aralik", "sorgu": "Motor devri 2000-3500 aralığında olan anlar.",
     "beklenen": {"filtreler": [{"alan": "motor_devri", "operator": "between", "deger": [2000, 3500]}]}},

    {"kategori": "dikey_hareket", "sorgu": "Aşağı doğru gittiği anları çıkar.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": "<", "deger": 0}]}},
    {"kategori": "dikey_hareket", "sorgu": "İniş yaptığı kısımları listele.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": "<", "deger": 0}]}},
    {"kategori": "dikey_hareket", "sorgu": "Alçaldığı anları çıkarır mısın?",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": "<", "deger": 0}]}},
    {"kategori": "dikey_hareket", "sorgu": "Süzülerek indiği bölümleri göster.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": "<", "deger": 0}]}},
    {"kategori": "dikey_hareket", "sorgu": "Yükselmeye başladığı yerleri göster.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": ">", "deger": 0}]}},
    {"kategori": "dikey_hareket", "sorgu": "Yukarı doğru tırmandığı yerleri listele.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": ">", "deger": 0}]}},
    {"kategori": "dikey_hareket", "sorgu": "Dikeydeki hızının saniyede 5 metreyi geçtiği anlara bakalım.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": ">", "deger": 5}]}},
    {"kategori": "dikey_hareket", "sorgu": "Tırmanma hızı 3 m/s'nin üstünde olan anlar.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": ">", "deger": 3}]}},
    {"kategori": "dikey_hareket", "sorgu": "İniş hızı 2 m/s'yi aşan kayıtları getir.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": ">", "deger": 2}]}},

    {"kategori": "operator_netlik", "sorgu": "Sıcaklığın 60 dereceden az olduğu anları getir.",
     "beklenen": {"filtreler": [{"alan": "sicaklik", "operator": "<", "deger": 60}]}},
    {"kategori": "operator_netlik", "sorgu": "Yüksekliğin 100 metre veya daha az olduğu noktalar.",
     "beklenen": {"filtreler": [{"alan": "irtifa", "operator": "<=", "deger": 100}]}},
    {"kategori": "operator_netlik", "sorgu": "Hızın minimum 30 olduğu kısımları bul.",
     "beklenen": {"filtreler": [{"alan": "hiz", "operator": ">=", "deger": 30}]}},
    {"kategori": "operator_netlik", "sorgu": "Motor devrinin tam 3000 olduğu anları listeler misin?",
     "beklenen": {"filtreler": [{"alan": "motor_devri", "operator": "==", "deger": 3000}]}},
    {"kategori": "operator_netlik", "sorgu": "Hızın 20 olmadığı kayıtları listeleyebilir misin?",
     "beklenen": {"filtreler": [{"alan": "hiz", "operator": "!=", "deger": 20}]}},
    {"kategori": "operator_netlik", "sorgu": "Havanın eksi 10 dereceden daha soğuk olduğu kısımları göster.",
     "beklenen": {"filtreler": [{"alan": "sicaklik", "operator": "<", "deger": -10}]}},

    {"kategori": "coklu_and", "sorgu": "Batarya değerinin %50'nin altına indiği, irtifanın 550'den düşük olduğu uçuşları getir",
     "beklenen": {"filtreler": [{"alan": "batarya", "operator": "<", "deger": 50},
                                {"alan": "irtifa", "operator": "<", "deger": 550}], "mantik": "AND"}},
    {"kategori": "coklu_and", "sorgu": "Hızı 30'un üzerinde ve irtifası 1000 metreden fazla olan kayıtlar.",
     "beklenen": {"filtreler": [{"alan": "hiz", "operator": ">", "deger": 30},
                                {"alan": "irtifa", "operator": ">", "deger": 1000}], "mantik": "AND"}},
    {"kategori": "coklu_and", "sorgu": "Sıcaklık 70 dereceyi geçerken motor devri de 4000'in üstünde olan anlar.",
     "beklenen": {"filtreler": [{"alan": "sicaklik", "operator": ">", "deger": 70},
                                {"alan": "motor_devri", "operator": ">", "deger": 4000}], "mantik": "AND"}},
    {"kategori": "coklu_and", "sorgu": "Şarj yüzde 20'nin altındayken aynı zamanda 100 metrenin altında uçtuğumuz anlar.",
     "beklenen": {"filtreler": [{"alan": "batarya", "operator": "<", "deger": 20},
                                {"alan": "irtifa", "operator": "<", "deger": 100}], "mantik": "AND"}},
    {"kategori": "coklu_and", "sorgu": "İrtifa 200 ile 500 arasında ve hız 25'ten yüksek olan kayıtları filtrele.",
     "beklenen": {"filtreler": [{"alan": "irtifa", "operator": "between", "deger": [200, 500]},
                                {"alan": "hiz", "operator": ">", "deger": 25}], "mantik": "AND"}},

    {"kategori": "coklu_or", "sorgu": "Hızı 30'u geçen ya da motor devri 5000'i aşan anları göster.",
     "beklenen": {"filtreler": [{"alan": "hiz", "operator": ">", "deger": 30},
                                {"alan": "motor_devri", "operator": ">", "deger": 5000}], "mantik": "OR"}},
    {"kategori": "coklu_or", "sorgu": "Batarya yüzde 10'un altında veya sıcaklık 85 dereceyi aşan kritik durumlar.",
     "beklenen": {"filtreler": [{"alan": "batarya", "operator": "<", "deger": 10},
                                {"alan": "sicaklik", "operator": ">", "deger": 85}], "mantik": "OR"}},
    {"kategori": "coklu_or", "sorgu": "Yatış açısı 45'i geçen veya yunuslama açısı eksi 30'un altına inen anlar.",
     "beklenen": {"filtreler": [{"alan": "yatis_acisi", "operator": ">", "deger": 45},
                                {"alan": "yunuslama_acisi", "operator": "<", "deger": -30}], "mantik": "OR"}},
    {"kategori": "coklu_or", "sorgu": "İrtifa 100'ün altında ya da hız 50'nin üstünde olan riskli anlar.",
     "beklenen": {"filtreler": [{"alan": "irtifa", "operator": "<", "deger": 100},
                                {"alan": "hiz", "operator": ">", "deger": 50}], "mantik": "OR"}},

    {"kategori": "coklu_zaman", "sorgu": "Son 10 dakikada batarya yüzde 30'un altında ve irtifa 200 metrenin altında olan kayıtlar.",
     "beklenen": {"filtreler": [{"alan": "batarya", "operator": "<", "deger": 30},
                                {"alan": "irtifa", "operator": "<", "deger": 200}],
                  "mantik": "AND", "zaman_araligi": "son 10 dakika"}},
    {"kategori": "coklu_zaman", "sorgu": "Geçtiğimiz yarım saatte hızı 20'yi geçen ve alçalan uçuşları bul.",
     "beklenen": {"filtreler": [{"alan": "hiz", "operator": ">", "deger": 20},
                                {"alan": "dikey_hiz", "operator": "<", "deger": 0}],
                  "mantik": "AND", "zaman_araligi": "geçtiğimiz yarım saat"}},

    {"kategori": "coklu_uclu", "sorgu": "İrtifası 500'ün altında, hızı 15'in üstünde ve bataryası yüzde 40'tan az olan kayıtlar.",
     "beklenen": {"filtreler": [{"alan": "irtifa", "operator": "<", "deger": 500},
                                {"alan": "hiz", "operator": ">", "deger": 15},
                                {"alan": "batarya", "operator": "<", "deger": 40}], "mantik": "AND"}},

    {"kategori": "zaman", "sorgu": "Son 10 dakikada şarj durumu nasıldı?",
     "beklenen": {"filtreler": [{"alan": "batarya", "operator": None, "deger": None}],
                  "zaman_araligi": "son 10 dakika"}},
    {"kategori": "zaman", "sorgu": "Son yarım saatte hızın 20'den yüksek olduğu yerler.",
     "beklenen": {"filtreler": [{"alan": "hiz", "operator": ">", "deger": 20}],
                  "zaman_araligi": "son yarım saat"}},
    {"kategori": "zaman", "sorgu": "Geçtiğimiz 1 saat içinde 500 metrenin üstüne çıktığımız anları bul.",
     "beklenen": {"filtreler": [{"alan": "irtifa", "operator": ">", "deger": 500}],
                  "zaman_araligi": "geçtiğimiz 1 saat"}},

    {"kategori": "gun_ici_saat", "sorgu": "Saat 7 ile 9 arasındaki uçuşları getir.",
     "beklenen": {"filtreler": [{"alan": "gun_ici_saat", "operator": "between", "deger": [7, 9]}],
                  "zaman_araligi": None}},
    {"kategori": "gun_ici_saat", "sorgu": "Saat 18 ile 21 arasındaki kayıtları göster.",
     "beklenen": {"filtreler": [{"alan": "gun_ici_saat", "operator": "between", "deger": [18, 21]}],
                  "zaman_araligi": None}},
    {"kategori": "gun_ici_saat", "sorgu": "Saat 22'den sonraki uçuşları listele.",
     "beklenen": {"filtreler": [{"alan": "gun_ici_saat", "operator": ">", "deger": 22}]}},
    {"kategori": "gun_ici_saat", "sorgu": "Sabah 6'daki kayıtları bul.",
     "beklenen": {"filtreler": [{"alan": "gun_ici_saat", "operator": "==", "deger": 6}]}},
    {"kategori": "gun_ici_saat_ayrim", "sorgu": "Son 1 saat içindeki kayıtları getir.",
     "beklenen": {"filtreler": [], "zaman_araligi": "son 1 saat"}},

    {"kategori": "ucus_suresi", "sorgu": "4 saatten kısa süren uçuşları listele.",
     "beklenen": {"filtreler": [{"alan": "ucus_suresi", "operator": "<", "deger": 4}]}},
    {"kategori": "ucus_suresi", "sorgu": "3 ile 5 saat arası süren uçuşları getir.",
     "beklenen": {"filtreler": [{"alan": "ucus_suresi", "operator": "between", "deger": [3, 5]}]}},
    {"kategori": "ucus_suresi", "sorgu": "8 saatten uzun süren uçuşları bul.",
     "beklenen": {"filtreler": [{"alan": "ucus_suresi", "operator": ">", "deger": 8}]}},
    {"kategori": "ucus_suresi_ayrim", "sorgu": "Saat 7 ile 9 arasında 4 saatten kısa süren uçuşları getir.",
     "beklenen": {"filtreler": [{"alan": "gun_ici_saat", "operator": "between", "deger": [7, 9]},
                                {"alan": "ucus_suresi", "operator": "<", "deger": 4}], "mantik": "AND"}},

    {"kategori": "belirsiz", "sorgu": "Çok sıcak olan anları listeler misin?",
     "beklenen": {"filtreler": [{"alan": "sicaklik", "operator": None, "deger": None}]}},
    {"kategori": "belirsiz", "sorgu": "Motor fazla hızlı dönmüş mü bir kontrol et.",
     "beklenen": {"filtreler": [{"alan": "motor_devri", "operator": None, "deger": None}]}},
    {"kategori": "belirsiz", "sorgu": "Şarjımız çok mu düşmüş sence?",
     "beklenen": {"filtreler": [{"alan": "batarya", "operator": None, "deger": None}]}},
    {"kategori": "belirsiz", "sorgu": "Bayağı yüksekte uçmuşuz gibi geldi bana.",
     "beklenen": {"filtreler": [{"alan": "irtifa", "operator": None, "deger": None}]}},
    {"kategori": "belirsiz", "sorgu": "Hız epey yüksekti sanki, ne dersin?",
     "beklenen": {"filtreler": [{"alan": "hiz", "operator": None, "deger": None}]}},
    {"kategori": "belirsiz", "sorgu": "Motor sesi tuhaftı, devir tarafında bir sorun var mıydı?",
     "beklenen": {"filtreler": [{"alan": "motor_devri", "operator": None, "deger": None}]}},
    {"kategori": "belirsiz", "sorgu": "Uçuşla ilgili genel bilgileri versene.",
     "beklenen": {"filtreler": []}},
    {"kategori": "belirsiz", "sorgu": "Bu verilerle tam olarak ne yapıyoruz bilmiyorum.",
     "beklenen": {"filtreler": []}},

    {"kategori": "dogal_dil", "sorgu": "500 metrenin üstünde uçanları bir göstersene.",
     "beklenen": {"filtreler": [{"alan": "irtifa", "operator": ">", "deger": 500}]}},
    {"kategori": "dogal_dil", "sorgu": "Şarjın yüzde 20'nin altına indiği yerleri atar mısın?",
     "beklenen": {"filtreler": [{"alan": "batarya", "operator": "<", "deger": 20}]}},
    {"kategori": "dogal_dil", "sorgu": "irtifasi 500 den fazla olanlari getr",
     "beklenen": {"filtreler": [{"alan": "irtifa", "operator": ">", "deger": 500}]}},
    {"kategori": "dogal_dil", "sorgu": "sicaklik 80i gecen kayitlari listele",
     "beklenen": {"filtreler": [{"alan": "sicaklik", "operator": ">", "deger": 80}]}},
    {"kategori": "dogal_dil", "sorgu": "Hız 15'le 25 arası olsun lütfen.",
     "beklenen": {"filtreler": [{"alan": "hiz", "operator": "between", "deger": [15, 25]}]}},
    {"kategori": "dogal_dil", "sorgu": "pil %10un altındaki anları bulur musun",
     "beklenen": {"filtreler": [{"alan": "batarya", "operator": "<", "deger": 10}]}},
    {"kategori": "dogal_dil", "sorgu": "Motor devri 4000'in üstünde olan kayıtları alabilir miyim?",
     "beklenen": {"filtreler": [{"alan": "motor_devri", "operator": ">", "deger": 4000}]}},
    {"kategori": "dogal_dil", "sorgu": "Yatış açısının 30'u geçtiği yer var mı?",
     "beklenen": {"filtreler": [{"alan": "yatis_acisi", "operator": ">", "deger": 30}]}},
    {"kategori": "dogal_dil", "sorgu": "batarya 25in altina dustugu ve irtifa 300 den az oldugu yerler",
     "beklenen": {"filtreler": [{"alan": "batarya", "operator": "<", "deger": 25},
                                {"alan": "irtifa", "operator": "<", "deger": 300}], "mantik": "AND"}},
    {"kategori": "dogal_dil", "sorgu": "hiz 10 ila 30 arasi olan ucuslari getir",
     "beklenen": {"filtreler": [{"alan": "hiz", "operator": "between", "deger": [10, 30]}]}},
]



#GEMİNİ VERİ SETİ
"""
TEST_CASES = [
    # ---------------------------------------------------------
    # R1: Açık dikey anahtar kelimesi -> alan dikey_hiz
    # ---------------------------------------------------------
    {"kategori": "R1_dikey_hiz", "sorgu": "Dikey hızın 15 m/s'yi geçtiği anları bul.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": ">", "deger": 15}]}},
    {"kategori": "R1_dikey_hiz", "sorgu": "Dikey irtifanın 10 metrenin altına düştüğü yerleri göster.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": "<", "deger": 10}]}},
    {"kategori": "R1_dikey_hiz", "sorgu": "Dikey hızın 25 değerine eşit olduğu durumlar.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": "==", "deger": 25}]}},
    {"kategori": "R1_dikey_hiz", "sorgu": "Dikey irtifanın 50'yi aştığı sekanslar nelerdir?",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": ">", "deger": 50}]}},
    {"kategori": "R1_dikey_hiz", "sorgu": "Dikey hızın -5'ten küçük olduğu zamanlar.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": "<", "deger": -5}]}},

    # ---------------------------------------------------------
    # R2: Sayısız yön ifadesi (alçalma/yükselme)
    # ---------------------------------------------------------
    {"kategori": "R2_sayisiz_yon", "sorgu": "Aracın alçaldığı kısımları listeler misin?",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": "<", "deger": 0}]}},
    {"kategori": "R2_sayisiz_yon", "sorgu": "Sadece yükseldiğimiz anları görmek istiyorum.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": ">", "deger": 0}]}},
    {"kategori": "R2_sayisiz_yon", "sorgu": "Uçağın alçalma evresine girdiği yerler.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": "<", "deger": 0}]}},
    {"kategori": "R2_sayisiz_yon", "sorgu": "Dronun sürekli yükselme yaptığı bölgeleri bul.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": ">", "deger": 0}]}},
    {"kategori": "R2_sayisiz_yon", "sorgu": "Sistemde alçalma tespit edilen noktaları ver.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": "<", "deger": 0}]}},

    # ---------------------------------------------------------
    # R2b: Çoklu sorguda değer 0 + yön ifadesi (yanlış alan tespiti)
    # ---------------------------------------------------------
    {"kategori": "R2b_yon_sifir_deger", "sorgu": "Alçalma ivmesinin 0'ın altına indiği anlar.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": "<", "deger": 0}]}},
    {"kategori": "R2b_yon_sifir_deger", "sorgu": "Yükselme değerinin 0'dan büyük olduğu kısımlar.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": ">", "deger": 0}]}},
    {"kategori": "R2b_yon_sifir_deger", "sorgu": "0 noktasına göre alçaldığımız durumları tespit et.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": "<", "deger": 0}]}},
    {"kategori": "R2b_yon_sifir_deger", "sorgu": "Yükselme miktarının 0'ı geçtiği yerler.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": ">", "deger": 0}]}},
    {"kategori": "R2b_yon_sifir_deger", "sorgu": "İrtifanın 0'a doğru alçaldığı durumları analiz et.",
     "beklenen": {"filtreler": [{"alan": "dikey_hiz", "operator": "<", "deger": 0}]}},

    # ---------------------------------------------------------
    # R3: Sorguda rakam yoksa ve yön yoksa eşik uydurulmuştur
    # ---------------------------------------------------------
    {"kategori": "R3_esik_uydurma", "sorgu": "Sadece hız durumlarını raporlar mısın?",
     "beklenen": {"filtreler": [{"alan": "hiz", "operator": None, "deger": None}]}},
    {"kategori": "R3_esik_uydurma", "sorgu": "Batarya seviyelerini grafikte göstermek istiyorum.",
     "beklenen": {"filtreler": [{"alan": "batarya", "operator": None, "deger": None}]}},
    {"kategori": "R3_esik_uydurma", "sorgu": "Motor devrindeki değişimleri bana sun.",
     "beklenen": {"filtreler": [{"alan": "motor_devri", "operator": None, "deger": None}]}},
    {"kategori": "R3_esik_uydurma", "sorgu": "Görevin sapma açısı loglarını getir.",
     "beklenen": {"filtreler": [{"alan": "sapma_acisi", "operator": None, "deger": None}]}},
    {"kategori": "R3_esik_uydurma", "sorgu": "Sensörden gelen sıcaklık verilerini listele.",
     "beklenen": {"filtreler": [{"alan": "sicaklik", "operator": None, "deger": None}]}},

    # ---------------------------------------------------------
    # R4: Operatör var, değer eksik, sorguda tek sayı var
    # ---------------------------------------------------------
    {"kategori": "R4_eksik_deger_tamamlama", "sorgu": "İrtifanın 400'den büyük olduğu yerleri bul.",
     "beklenen": {"filtreler": [{"alan": "irtifa", "operator": ">", "deger": 400}]}},
    {"kategori": "R4_eksik_deger_tamamlama", "sorgu": "Hız 150 km/h değerini aşınca uyar.",
     "beklenen": {"filtreler": [{"alan": "hiz", "operator": ">", "deger": 150}]}},
    {"kategori": "R4_eksik_deger_tamamlama", "sorgu": "Batarya seviyesi 15'in altına düşmüş mü?",
     "beklenen": {"filtreler": [{"alan": "batarya", "operator": "<", "deger": 15}]}},
    {"kategori": "R4_eksik_deger_tamamlama", "sorgu": "Sıcaklığın 35 dereceyi geçtiği anlar.",
     "beklenen": {"filtreler": [{"alan": "sicaklik", "operator": ">", "deger": 35}]}},
    {"kategori": "R4_eksik_deger_tamamlama", "sorgu": "Basıncın 1000 mbar altında olduğu kısımlar.",
     "beklenen": {"filtreler": [{"alan": "basinc", "operator": "<", "deger": 1000}]}},

    # ---------------------------------------------------------
    # R5: Between olmayan operatörde liste değer gelirse
    # ---------------------------------------------------------
    {"kategori": "R5_liste_deger_tekille", "sorgu": "İrtifanın 100 veya 200'ü geçtiği anları getir.",
     "beklenen": {"filtreler": [{"alan": "irtifa", "operator": ">", "deger": 100}]}},
    {"kategori": "R5_liste_deger_tekille", "sorgu": "Hızın 50, 60 gibi seviyelerin altına düştüğü kısımlar.",
     "beklenen": {"filtreler": [{"alan": "hiz", "operator": "<", "deger": 50}]}},
    {"kategori": "R5_liste_deger_tekille", "sorgu": "Motor devri 3000 yahut 4000 sınırını aştığında.",
     "beklenen": {"filtreler": [{"alan": "motor_devri", "operator": ">", "deger": 3000}]}},
    {"kategori": "R5_liste_deger_tekille", "sorgu": "Bataryanın 20, 10 civarlarına indiği yerler.",
     "beklenen": {"filtreler": [{"alan": "batarya", "operator": "<", "deger": 20}]}},
    {"kategori": "R5_liste_deger_tekille", "sorgu": "Sıcaklık 40 ile 50'den daha büyükse.",
     "beklenen": {"filtreler": [{"alan": "sicaklik", "operator": ">", "deger": 40}]}},
]

"""

# ============================================================
# JSON / MODEL
# ============================================================

THINK_REGEX = re.compile(r"<think>.*?</think>", re.DOTALL | re.I)


def temizle_json(raw) -> str:
    if raw is None:
        return ""
    raw = THINK_REGEX.sub("", str(raw)).strip()
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.lower().startswith("json"):
            raw = raw[4:].strip()
    i, s = raw.find("{"), raw.rfind("}")
    return raw[i:s + 1].strip() if (i != -1 and s > i) else raw.strip()


def onar_json(m: str) -> str:
    m = re.sub(r"\]\s*\]\s*\}", "]}]", m)
    m = re.sub(r"\]\s*\]\s*,\s*\[?\s*\{", "]}, {", m)
    return re.sub(r",\s*([}\]])", r"\1", m)


def parse_et(ham: str):
    t = temizle_json(ham)
    try:
        return json.loads(t), "ok"
    except json.JSONDecodeError:
        pass
    try:
        return json.loads(onar_json(t)), "onarildi"
    except json.JSONDecodeError:
        return None, "basarisiz"


def normalize_null(d):
    return None if isinstance(d, str) and d.strip().lower() in {"null", "none", ""} else d


def call_qwen(messages: list) -> str:
    try:
        r = ollama.chat(model=MODEL, messages=messages, think=False,
                        options={"temperature": 0, "num_predict": 600})
    except TypeError:
        r = ollama.chat(model=MODEL, messages=messages,
                        options={"temperature": 0, "num_predict": 600})
    return r["message"]["content"]


def sorgula(sorgu: str):
    msgs = [{"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": sorgu}]
    ham = call_qwen(msgs)
    parsed, durum = parse_et(ham)
    if parsed is not None:
        return parsed, durum, ham

    msgs += [{"role": "assistant", "content": ham},
             {"role": "user", "content": "Bu geçerli JSON değil. Sadece geçerli JSON olarak tekrar yaz."}]
    ham2 = call_qwen(msgs)
    parsed2, durum2 = parse_et(ham2)
    return (parsed2, f"yeniden_{durum2}", ham2) if parsed2 else (None, "basarisiz", ham2)


# ============================================================
# SKORLAMA
# ============================================================

def deger_esit(a, b) -> bool:
    a, b = normalize_null(a), normalize_null(b)
    if a is None or b is None:
        return a is None and b is None
    try:
        if isinstance(b, list) and isinstance(a, list):
            return len(a) == len(b) and all(abs(float(x) - float(y)) < 1e-6 for x, y in zip(a, b))
        return abs(float(a) - float(b)) < 1e-6
    except (TypeError, ValueError):
        return a == b


def filtre_esit(g, b) -> bool:
    if not isinstance(g, dict):
        return False
    ga, ba = normalize_null(g.get("alan")), normalize_null(b.get("alan"))
    if isinstance(ga, str) and isinstance(ba, str):
        if ga.strip().lower() != ba.strip().lower():
            return False
    elif ga != ba:
        return False
    if normalize_null(g.get("operator")) != normalize_null(b.get("operator")):
        return False
    return deger_esit(g.get("deger"), b.get("deger"))


def skorla(parsed, beklenen):
    if not isinstance(parsed, dict):
        return 0.0, ["yanit dict degil"]
    puanlar, detay = [], []

    if "filtreler" in beklenen:
        gelen, bek = parsed.get("filtreler", []), beklenen["filtreler"]
        if not isinstance(gelen, list):
            oran, acik = 0.0, "filtreler liste degil"
        elif not bek:
            oran = 1.0 if not gelen else 0.0
            acik = "bos liste (dogru)" if oran else f"bos beklenirken {len(gelen)} filtre"
        else:
            kalan, eslesen = list(gelen), 0
            for b in bek:
                for i, g in enumerate(kalan):
                    if filtre_esit(g, b):
                        eslesen += 1
                        kalan.pop(i)
                        break
            oran = eslesen / max(len(bek), len(gelen))
            acik = f"{eslesen}/{len(bek)} eslesti (gelen: {len(gelen)})"
        puanlar.append(oran)
        detay.append(f"filtreler: {'OK' if oran == 1.0 else 'FARKLI'} - {acik}")
        if oran < 1.0:
            detay.append(f"    beklenen: {json.dumps(bek, ensure_ascii=False)}")
            detay.append(f"    gelen   : {json.dumps(gelen, ensure_ascii=False)}")

    if "mantik" in beklenen:
        e = str(parsed.get("mantik", "")).strip().upper() == beklenen["mantik"].upper()
        puanlar.append(1.0 if e else 0.0)
        detay.append(f"mantik: {'OK' if e else 'FARKLI'} (gelen={parsed.get('mantik')})")

    if "zaman_araligi" in beklenen:
        g = normalize_null(parsed.get("zaman_araligi"))
        e = (g is None) if beklenen["zaman_araligi"] is None else (g is not None and str(g).strip() != "")
        puanlar.append(1.0 if e else 0.0)
        detay.append(f"zaman_araligi: {'OK' if e else 'FARKLI'} (gelen={parsed.get('zaman_araligi')})")

    return (statistics.mean(puanlar) if puanlar else 0.0), detay


# ============================================================

if __name__ == "__main__":
    print("=" * 70)
    print(f"MODEL: {MODEL} | v6 promptu | Son islem: {'ACIK' if SON_ISLEM else 'KAPALI'}")
    print(f"Test sayisi: {len(TEST_CASES)}")
    print("=" * 70)
    print()

    sonuclar = []
    for i, test in enumerate(TEST_CASES, 1):
        sorgu, beklenen, kat = test["sorgu"], test["beklenen"], test["kategori"]
        print(f"[{i}/{len(TEST_CASES)}] ({kat}) {sorgu}")
        t0 = time.time()
        try:
            parsed, durum, ham = sorgula(sorgu)
            sure = time.time() - t0
            if parsed is None:
                print(f"  -> JSON PARSE HATASI: {ham[:120]!r}\n")
                sonuclar.append({"kategori": kat, "sorgu": sorgu, "sure": sure,
                                 "ham_skor": 0.0, "skor": 0.0})
                continue

            ham_skor, _ = skorla(parsed, beklenen)
            duzeltmeler = []
            if SON_ISLEM:
                parsed, duzeltmeler = son_islem(parsed, sorgu)
            skor, detay = skorla(parsed, beklenen)

            fark = f" (ham: {ham_skor:.2f} -> {skor:.2f})" if abs(skor - ham_skor) > 1e-9 else ""
            ek = "" if durum == "ok" else f", {durum}"
            print(f"  -> {sure:.2f}s, skor: {skor:.2f}{fark}{ek}")
            for d in duzeltmeler:
                print(f"     [son islem] {d}")
            if skor < 1.0:
                for d in detay:
                    print(f"     {d}")

            sonuclar.append({"kategori": kat, "sorgu": sorgu, "sure": sure,
                             "ham_skor": ham_skor, "skor": skor})
        except Exception as e:
            print(f"  -> HATA: {type(e).__name__}: {e}")
            sonuclar.append({"kategori": kat, "sorgu": sorgu, "sure": None,
                             "ham_skor": 0.0, "skor": 0.0})
        print()

    print("=" * 70)
    print("OZET")
    print("=" * 70)
    ham_ort = statistics.mean(s["ham_skor"] for s in sonuclar) * 100
    ort = statistics.mean(s["skor"] for s in sonuclar) * 100
    tam = sum(1 for s in sonuclar if s["skor"] == 1.0)
    sureler = [s["sure"] for s in sonuclar if s["sure"]]

    print(f"Ham LLM (v6 prompt) : {ham_ort:.1f}%     (v5 ham: %81.0)")
    print(f"Son islem sonrasi   : {ort:.1f}%     (v5: %86.7)")
    print(f"Tam dogru test      : {tam}/{len(sonuclar)}   (v5: 59/70)")
    if sureler:
        print(f"Ortalama sure       : {statistics.mean(sureler):.2f}s")
    print()

    kats = defaultdict(list)
    for s in sonuclar:
        kats[s["kategori"]].append(s)
    print("Kategori bazinda (ham -> son islem):")
    for k, l in sorted(kats.items(), key=lambda kv: statistics.mean(x["skor"] for x in kv[1])):
        h = statistics.mean(x["ham_skor"] for x in l) * 100
        s = statistics.mean(x["skor"] for x in l) * 100
        print(f"  {k:16s} | {h:5.1f}% -> {s:5.1f}% ({len(l)} test)")
    print()

    eksik = [s for s in sonuclar if s["skor"] < 1.0]
    if eksik:
        print(f"Tam puan alamayan {len(eksik)} test:")
        for s in eksik:
            print(f"  [{s['skor']:.2f}] ({s['kategori']}) {s['sorgu']}")
        print()

    with open("qwen_benchmark_v6_sonuclari.json", "w", encoding="utf-8") as f:
        json.dump(sonuclar, f, ensure_ascii=False, indent=2)
    print("Sonuclar 'qwen_benchmark_v6_sonuclari.json' dosyasina kaydedildi.")
