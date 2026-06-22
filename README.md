# V2Ray Finder — Blazor WebAssembly

Port of [v2ray-finder](https://github.com/alisadeghiaghili/v2ray-finder) in **C# / .NET 8**.

پورت کامل **V2Ray Finder** به **Blazor WebAssembly** — همه چیز در مرورگر اجرا می‌شود، هیچ سرور بک‌اندی لازم نیست.

---

## ویژگی‌ها

- ✅ اجرای کامل در مرورگر (WebAssembly / Client-Side)
- ✅ رابط کاربری فارسی RTL با تم دارک ترمینال
- ✅ دریافت موازی از ۱۸+ منبع عمومی با progress bar زنده
- ✅ پشتیبانی از پروکسی CORS (برای دور زدن محدودیت مرورگر)
- ✅ پارس خودکار vmess / vless / trojan / shadowsocks
- ✅ امتیازدهی A–F و مرتب‌سازی
- ✅ جستجو و فیلتر پروتکل
- ✅ صفحه‌بندی
- ✅ کپی یک کانفیگ / کپی همه / دانلود .txt
- ✅ Ctrl+C / دکمه توقف با CancellationToken

---

## محدودیت‌های WASM (در مقایسه با CLI)

| قابلیت | CLI (.NET) | Blazor WASM |
|--------|-----------|-------------|
| TCP Health Check | ✅ | ❌ (مرورگر اجازه نمی‌دهد) |
| همزمانی بالا | ✅ 50+ | ⚠️ حداکثر ۶ (محدودیت browser) |
| دسترسی مستقیم به URL | ✅ | ⚠️ نیاز به CORS proxy |

---

## ساختار پروژه

```
V2RayFinderBlazor/
├── src/
│   ├── V2RayFinder.Core.Wasm/       ← کتابخانه core سازگار با WASM
│   │   ├── Models.cs                ← مدل‌ها
│   │   ├── ConfigParser.cs          ← پارسر (SHA256، Regex، Base64)
│   │   ├── SubscriptionSources.cs   ← منابع + CORS proxy builder
│   │   ├── WasmFetcher.cs           ← HttpClient async (بدون TcpClient)
│   │   ├── ConfigScorer.cs          ← امتیازدهی A–F
│   │   └── WasmPipeline.cs          ← ارکستراتور با IProgress<T>
│   └── V2RayFinder.Blazor/          ← اپلیکیشن Blazor
│       ├── Pages/Home.razor         ← رابط کاربری اصلی
│       ├── Layout/MainLayout.razor
│       ├── wwwroot/
│       │   ├── index.html           ← host page + JS helpers
│       │   ├── css/app.css          ← تم دارک RTL
│       │   └── service-worker.js    ← PWA (اختیاری)
│       └── Program.cs
└── V2RayFinderBlazor.sln
```

---

## نصب و اجرا

### پیش‌نیازها
- .NET 8 SDK: https://dotnet.microsoft.com/download/dotnet/8.0

### اجرای محلی

```bash
cd src/V2RayFinder.Blazor
dotnet run
# باز کن: https://localhost:7080
```

### Build برای تولید

```bash
dotnet publish src/V2RayFinder.Blazor -c Release -o ./publish
# فایل‌های publish/wwwroot را روی هر وب‌سرور استاتیک آپلود کن
```

### دیپلوی رایگان

```bash
# GitHub Pages
dotnet publish -c Release
# محتوای publish/wwwroot را به شاخه gh-pages بفرست

# Netlify / Vercel
# فقط پوشه publish/wwwroot را آپلود کن
# Base URL = /  ،  Publish dir = wwwroot
```

> ⚠️ برای GitHub Pages باید یک فایل `404.html` هم کنار `index.html` بگذاری (کپی از index.html) تا SPA routing کار کند.

---

## نحوه استفاده از CORS Proxy

چون مرورگر نمی‌تواند مستقیماً به URL‌های cross-origin بدون هدر CORS دسترسی داشته باشد، از [corsproxy.io](https://corsproxy.io) استفاده می‌کنیم.

- **پروکسی CORS فعال** (پیش‌فرض): بهترین حالت برای دریافت
- **پروکسی CORS غیرفعال**: فقط اگر URL مستقیم هدر CORS داشته باشد کار می‌کند

اگر می‌خواهی از پروکسی خودت استفاده کنی، در `SubscriptionSources.cs` این خط را تغییر بده:

```csharp
private const string CorsProxy = "https://your-proxy.example.com/?";
```

---

## افزودن منبع جدید

در `SubscriptionSources.cs`:

```csharp
new("نام-منبع", "https://raw.githubusercontent.com/.../subscription.txt"),
```
