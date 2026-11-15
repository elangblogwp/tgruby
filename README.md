# Ruby UBot

Panel Telegram dan userbot untuk satu grup target per konfigurasi. Gunakan Python 3.11 atau lebih baru. Paket diuji pada Python 3.12.14 dengan Telethon 1.45.0.

## Instal di Ubuntu 24.04

Unggah arsip ke direktori home akun yang akan menjalankan bot. Ekstrak dan pasang dependensi dengan pip.

```bash
sudo apt update
sudo apt install -y python3-venv unzip
mkdir -p ~/ruby-ubot
unzip -o ~/ruby-ubot.zip -d ~/ruby-ubot
cd ~/ruby-ubot
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
install -m 600 .env.example .env
nano .env
.venv/bin/python main.py
```

Isi API_ID dan API_HASH milik aplikasi Telegram Anda, BOT_TOKEN dari BotFather, dan OWNER_ID dengan ID numerik akun pemilik. SEND_TIMEOUT mengatur batas tunggu permintaan jaringan dalam detik. Nilai awal 30.

Untuk memperbarui instalasi yang sudah berjalan, hentikan proses atau layanan yang menjalankannya. Buat salinan .env, data.json, dan direktori sessions saat proses telah berhenti, lalu ekstrak seluruh arsip ke direktori instalasi. Jalankan pip di virtual environment yang biasa dipakai layanan dan mulai kembali prosesnya. File .env.example ditujukan untuk instalasi baru. Instalasi lama menggunakan .env, data.json, dan sesi di direktori yang sama. SQLite deliveries.sqlite3 menyimpan catatan pengiriman, giliran dan pembatasan Telegram. Menghapusnya dapat menyebabkan post lama dibalas lagi.

## Bantuan dan login

Kirim /help atau /start ke panel. Tombol kategori membuka halaman di pesan yang sama. Kembali membuka halaman utama. Pengguna yang diberi akses dapat mengelola workernya. Akun worker dapat membaca bantuan perintah grup, promosi, dan status. Kategori Pengguna khusus pemilik.

Kirim /login di chat pribadi panel, masukkan jumlah worker (minimal dua), lalu nomor dengan kode negara, kode Telegram, dan password 2FA bila diminta. Ulangi untuk setiap slot. Saat seluruh worker tersambung, pengaturan berlaku bersama untuk mereka. /logout memutuskan seluruh worker pengguna dan menghapus sesi worker lokal.

Pada akun worker, .help menampilkan daftar singkat perintah dan tautan ke panel. Konfirmasi perubahan teks, harga, dan keyword cukup satu baris. Kirim perintah tanpa argumen untuk membaca nilai yang disimpan. Pratinjau menampilkan teks tanpa emoji. Teks promosi dan harga yang dikirim ke grup mempertahankan isi yang Anda masukkan.

| Letak | Perintah | Fungsi |
| --- | --- | --- |
| Panel | /start, /help | Bantuan dengan tombol kategori dan Kembali |
| Panel | /login, /logout | Hubungkan atau putuskan worker |
| Panel pemilik | /users, /adduser ID, /deluser ID | Lihat, beri atau cabut akses |
| Worker | .help, .ping | Bantuan, status koneksi, jumlah worker, pembatasan dan waktu pengiriman terakhir |
| Worker | .listgc, .addgc ID, .rmgc ID | Lihat, pilih atau hapus grup target |
| Worker | .keyword noktel, samsung | Tetapkan keyword aktif. .keyword clear mengosongkan daftar |
| Worker | .text ready stock! | Tetapkan promosi. Pisahkan variasi dengan garis vertikal. .text clear menghapus teks |
| Worker | .addprice noktel 15k | Simpan harga khusus noktel. .addprice noktel menampilkan harga. .addprice noktel clear menghapusnya |
| Worker | .open, .close | Aktifkan atau jeda autoreply |

Harga menerima beberapa baris. Untuk keyword noktel, harga 15k dan promosi ready stock!, pesan menjadi 15k, satu baris kosong, lalu ready stock!. Batas 99 karakter dihitung pada teks atau caption post masuk menggunakan panjang string Unicode Python. Post yang mengandung t.u, post lama sebelum jendela aktif, dan kiriman yang bukan penerusan channel sesuai aturan grup disaring. Batas promosi mengikuti batas terpisah 4.096 karakter. Emoji pengarah di awal post dilewati. Emoji di badan post dapat ditambahkan di akhir promosi, masing-masing satu kali. Bila hanya tambahan emoji yang melewati batas pesan, worker mencari variasi yang muat lalu memakai promosi utuh bila perlu.

## Pengiriman dan pemulihan

- Worker bergiliran setiap dua penugasan. Semua worker yang dikonfigurasi harus tersambung agar pengiriman aktif. Dilakukan satu klaim SQLite untuk satu post atau album dalam grup sebelum pengiriman.
- Jalur kiriman langsung tidak memasang jeda buatan. FloodWait dan slow mode menjeda seluruh pool terkait sesuai durasi Telegram. Masalah akses atau larangan permanen menghentikan pengiriman. Koneksi yang putus dipulihkan pada slot yang sama.
- Saat ada FloodWait, paling banyak 32 post baru dipertahankan sebagai pekerjaan yang sedang menunggu. Post yang lain diperiksa kembali dari riwayat setelah jeda. Upaya ulang setelah timeout memakai akun dan random_id yang sama. Bila status akhirnya tidak pasti, klaim dipertahankan untuk menghindari komentar ganda.
- Setiap 30 detik bot memeriksa riwayat target yang aktif. Satu worker membaca riwayat supergrup. Untuk grup biasa, setiap akun punya ID pesan lokalnya sendiri. Peringatan PersistentTimestampOutdatedError dari Telethon juga memicu pemeriksaan terarah. Pesan yang telah dihapus atau riwayat yang tak dapat diakses tetap membatasi pemulihan.

## Kode dan pengujian

main.py adalah entry point. Modul ruby_ubot/panel.py dan help.py menangani panel. commands.py menangani perintah worker. pool.py, history.py, dan ledger.py menangani pengiriman, pemulihan riwayat, serta klaim dan status SQLite. sessions.py dan connections.py menangani sesi pengguna. text.py, config.py, dan store.py menangani teks serta konfigurasi.

```bash
.venv/bin/python -m pip install -r requirements-test.txt
.venv/bin/python -m pytest -q
.venv/bin/python benchmarks/benchmark.py --case burst --posts 1000
.venv/bin/python benchmarks/benchmark.py --case mixed --posts 4000
.venv/bin/python benchmarks/benchmark.py --case history --posts 10000
```

Tambahkan `--baseline /lokasi/versi-sebelumnya/main.py` untuk menguji kode lama dengan beban yang sama. Jika mesin digunakan proses lain atau karakteristik disk berbeda, hasil dapat berubah.

### Hasil pengukuran lokal

Median sembilan proses per versi dan skenario, dengan urutan versi bergantian. Empat client simulasi mengamati post yang sama. Benchmark menggunakan database SQLite nyata. Burst dan campuran dijalankan dalam kelompok 100 post bersamaan. Latensi dihitung dari pembuatan event uji hingga panggilan kirim pada client simulasi, termasuk penjadwalan lokal. Puncak RSS mencakup interpreter, Telethon, dan alat ukur. Uji ini tidak mengukur latensi Internet, server Telegram, atau performa VPS Anda.

| Beban | Balasan lama / baru | p95 lama / baru (ms) | CPU lama / baru (ms) | RSS lama / baru (MiB) |
| --- | ---: | ---: | ---: | ---: |
| 1.000 post cocok | 1.000 / 1.000 | 4,129 / 6,775 | 45,30 / 45,45 | 57,64 / 57,89 |
| 4.000 post, 10% cocok | 400 / 400 | 3,046 / 2,883 | 127,75 / 123,63 | 57,27 / 57,52 |
| 10.000 post riwayat, 1% cocok | 100 / 100 | 0,758 / 0,543 | 73,23 / 58,02 | 56,64 / 56,78 |

Dalam profiling beban campuran, jumlah perhitungan identitas post turun dari 16.000 menjadi 400. Pada beban seluruhnya cocok, p95 bertambah. Transaksi SQLite dan penulisan konfigurasi berjalan di thread terpisah supaya event loop dapat melayani update ketika disk menunggu. Semua ID balasan di tiga skenario diperiksa terhadap himpunan post yang diharapkan.
