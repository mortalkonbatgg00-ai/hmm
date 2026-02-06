TRANSLATIONS = {
    # Status
    "Paid": "مدفوع",
    "Active": "نشط",
    "Overdue": "متأخر",
    "Expired": "منتهي",
    "Expiring Soon": "قارب على الانتهاء",
    # WhatsApp Queue
    "Pending": "قيد الانتظار",
    "Sending": "جاري الإرسال",
    "Accepted": "تم الإرسال",
    "Failed": "فشل الإرسال",
    "Expired": "منتهي",
    "Stopped": "موقوف",
    # Broadcast Targets
    "All Clients": "كل الزبائن",
    "Clients with Active Rentals": "زبائن لديهم إيجارات فعّالة",
    # General / Connection
    "Unknown": "غير معروف",
    "Offline": "غير متصل",
    "Online": "متصل",
    "Excellent": "ممتاز",
    "Medium": "متوسط",
    "Weak": "ضعيف",
    # UI Sections
    "Dashboard": "لوحة التحكم",
    "Clients": "الزبائن",
    "Rentals": "الإيجارات",
    "Notifications": "الإشعارات",
    "Broadcasts": "البث",
    "WhatsApp Sender (Experimental)": "مرسل واتساب (تجريبي)",
    "Status: Idle": "الحالة: متوقف",
    "Checking connection...": "جارٍ فحص الاتصال...",
    "Online •": "متصل •",
    "No attachment selected": "لا يوجد ملف مرفق",
}


def tr(value):
    if not isinstance(value, str):
        return value
    return TRANSLATIONS.get(value, value)
