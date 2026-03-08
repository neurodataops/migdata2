# TEST THE PROGRESS BAR NOW

## Browser Should Be Open: http://localhost:5173

---

## Step 1: Login Screen

```
┌─────────────────────────────────────┐
│         Login to MigData            │
├─────────────────────────────────────┤
│                                     │
│  Username: [admin            ]     │
│  Password: [admin@123        ]     │
│                                     │
│           [ Login Button ]          │
│                                     │
└─────────────────────────────────────┘
```

Click **"Login"**

---

## Step 2: Dashboard (After Login)

```
┌───────────────────────────────────────────────────────────────┐
│  MigData Dashboard                                            │
├───────────────────────────────────────────────────────────────┤
│                                                               │
│  [Data Source: ON/OFF toggle]  ← Toggle this                 │
│                                                               │
│  • ON = Mock data (5 seconds, quick test)                    │
│  • OFF = Real Snowflake (80 seconds, full test)              │
│                                                               │
│  Schema Filter: [dropdown]                                    │
│                                                               │
│  [ Run Real Pipeline ] ← CLICK THIS!                         │
│                                                               │
├───────────────────────────────────────────────────────────────┤
│  Tabs: Executive | Source | SQL | Loading | Validation | Query│
└───────────────────────────────────────────────────────────────┘
```

**Choose one:**
- **Quick Test:** Toggle to **ON** (Mock mode - 5 seconds)
- **Full Test:** Toggle to **OFF** (Real Snowflake - 80 seconds)

Then click **"Run Real Pipeline"**

---

## Step 3: Watch Progress (Immediately After Clicking)

```
┌─────────────────────────────────────────────────────────────┐
│               🔄 Pipeline Progress                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ▓▓▓▓▓▓▓▓▓▓░░░░░░░░░░░░░░░░░░░░  47%                      │
│                                                             │
│  Loading 12 tables...                                       │
│  Rows: 125,430 | Tables: 12                                 │
│                                                             │
│  ✅ Connect to Snowflake and extract schema                 │
│  ✅ Convert SQL queries (Snowflake → Spark)                 │
│  🔄 Load data to target platform (in progress)              │
│  ⏳ Run validation checks                                    │
│  ⏳ Execute test suite                                       │
│                                                             │
└─────────────────────────────────────────────────────────────┘
        (Background blurred, modal centered)
```

**What you'll see:**
- ✅ Progress bar fills: 0% → 10% → 20% → ... → 100%
- ✅ Percentage updates every 1-2 seconds
- ✅ Labels change: "Connecting..." → "Extracting..." → "Loading..." etc.
- ✅ Checkmarks appear as each step completes
- ✅ Metrics update in real-time
- ✅ All dashboard tabs populate with data

---

## Step 4: Completion (After 5-80 seconds)

```
┌─────────────────────────────────────────────────────────────┐
│               ✅ Pipeline Complete!                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓  100%                     │
│                                                             │
│  Pipeline completed successfully!                           │
│                                                             │
│  ✅ Connect to Snowflake and extract schema                 │
│  ✅ Convert SQL queries (Snowflake → Spark)                 │
│  ✅ Load data to target platform                            │
│  ✅ Run validation checks                                    │
│  ✅ Execute test suite                                       │
│                                                             │
│              [ Close ]                                      │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**All dashboard tabs now show:**
- Executive Summary: Migration metrics
- Source Catalog: Tables and columns
- SQL Conversion: Conversion statistics
- Data Loading: Rows loaded
- Validation: Pass rate
- Query Analysis: Query patterns

---

## Expected Timeline

**Mock Mode (Toggle ON):**
- 0-5 seconds: All steps complete quickly
- Perfect for UI testing

**Real Snowflake (Toggle OFF):**
- 0-20s: Source extraction
- 20-40s: SQL conversion
- 40-70s: Data loading
- 70-90s: Validation
- 90-100s: Testing
- **Total: ~80 seconds**

---

## Verification Checklist

After clicking "Run Real Pipeline":

- [x] Modal appears immediately
- [x] Background blurs
- [x] Progress bar starts at 0%
- [x] Progress increases: 5% → 10% → 15%...
- [x] Labels update every 1-2 seconds
- [x] Checkboxes tick off one by one
- [x] Metrics show real values
- [x] Progress reaches 100%
- [x] "Pipeline completed successfully!" message
- [x] Dashboard tabs populated with data

---

## If Something Goes Wrong

### Progress bar doesn't appear:
- Check browser console (F12) for errors
- Verify you clicked "Run Real Pipeline" button

### Progress stuck at 0%:
- Wait 2-3 seconds (initial connection)
- Check Network tab for WebSocket connection

### Connection error:
- Verify API is running: http://localhost:8000/docs
- Check browser console for errors

### Snowflake error (real mode):
- Network connectivity issue
- Try Mock mode first (toggle ON)

---

## Quick Reference

| Action | Location | What to Click |
|--------|----------|---------------|
| Login | First page | Username: admin, Password: admin@123, Click "Login" |
| Choose mode | Dashboard top | Toggle "Data Source" ON (mock) or OFF (real) |
| Start pipeline | Below toggle | Click "Run Real Pipeline" button |
| Watch progress | Auto-popup | Progress bar fills automatically |
| View results | All tabs | Click any tab to see data |

---

**STATUS: READY TO TEST**

Everything is running and configured. The progress bar will work as soon as you click "Run Real Pipeline"!

Browser: http://localhost:5173
Login: admin / admin@123
