# V4 Render — نسخة موحدة ونظيفة

هذه النسخة تحتوي على **V4 واحد** داخل ملف `bot.py`، بدون بقايا V2/V3 أو monkey-patching للأحداث.

## ما تم تنظيفه
- نقطة تشغيل واحدة `setup_hook`.
- نقطة معالجة واحدة `on_message` تجمع حماية الرسائل وميزات V4.
- `parse_duration` معرفة مرة واحدة فقط.
- `process_extended_message` معرفة مرة واحدة فقط.
- إزالة `V4MessageObserverCog` لتجنب وجود مستمع `on_message` إضافي؛ تتم مراقبة الرسائل من الحدث الرئيسي مباشرة.
- إزالة تعليقات وبقايا الـ Patch القديمة.
- الإبقاء على أنظمة الاقتصاد، الإدارة، التذاكر، الاقتراحات، XP، AutoMod، النسخ الاحتياطي، وأنظمة V4 في نفس البوت.

## Render
Build Command:
`pip install -r requirements.txt`

Start Command:
`python bot.py`

Environment Variable:
`DISCORD_TOKEN=توكن البوت`

يستمع خادم الويب على `0.0.0.0` وعلى `PORT` الذي توفره Render.

## Discord Developer Portal
فعّل عند الحاجة:
- Message Content Intent
- Server Members Intent

## ملاحظة قاعدة البيانات
يستخدم البوت SQLite محليًا (`bot.db`). على Render Free نظام الملفات مؤقت؛ إذا كانت البيانات مهمة، استخدم قاعدة بيانات خارجية أو Persistent Disk في خطة تدعمها.
