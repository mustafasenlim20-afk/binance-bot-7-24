cat > /usr/local/bin/botguncelle <<'XEOF'
#!/bin/bash
# AYAR: bot klasoru ve servis adi (asagidaki iki satiri kendi sunucuna gore duzelt)
KLASOR="/root/binance-bot-7-24"
SERVIS="bot"

cd "$KLASOR" || { echo "HATA: klasor yok: $KLASOR"; exit 1; }
echo "1) Bot durduruluyor..."
systemctl stop "$SERVIS" || { echo "HATA: servis bulunamadi: $SERVIS"; exit 1; }
echo "2) GitHub'dan yeni kod cekiliyor..."
git pull origin main || { echo "HATA: git pull basarisiz, bot BASLATILMADI"; exit 1; }
python3 -m py_compile app.py || { echo "HATA: kodda sozdizimi hatasi, bot BASLATILMADI"; exit 1; }
echo "3) Bot baslatiliyor..."
systemctl start "$SERVIS"
sleep 3
systemctl is-active "$SERVIS" && echo "TAMAM: bot calisiyor"
journalctl -u "$SERVIS" -n 15 --no-pager
XEOF
cat > /usr/local/bin/botdurdur <<'XEOF'
#!/bin/bash
systemctl stop bot && echo "bot durduruldu"
XEOF
cat > /usr/local/bin/botbaslat <<'XEOF'
#!/bin/bash
systemctl start bot && sleep 2 && systemctl is-active bot
XEOF
cat > /usr/local/bin/botlog <<'XEOF'
#!/bin/bash
journalctl -u bot -f --no-pager
XEOF
chmod +x /usr/local/bin/botguncelle /usr/local/bin/botdurdur /usr/local/bin/botbaslat /usr/local/bin/botlog
echo "Kurulum tamam. Kontrol:"; systemctl list-units --type=service | grep -i bot; ls /root | head -20
