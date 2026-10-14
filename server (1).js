const { default: makeWASocket, useMultiFileAuthState,
        DisconnectReason, delay, Browsers } = require("@whiskeysockets/baileys")
const pino    = require("pino")
const express = require("express")
const multer  = require("multer")
const csv     = require("csv-parser")
const QRCode  = require("qrcode")
const fs      = require("fs")
const https   = require("https")

const app    = express()
const upload = multer({ dest: "./uploads/" })
const PORT   = process.env.PORT || 3000

const CF = "./data/contacts.json"
const HF = "./data/history.json"
const YF = "./data/youtube.json"
const NF = "./data/numbers.json"

app.use(express.json())
app.use(express.static("public"))

if (!fs.existsSync("./data"))    fs.mkdirSync("./data", { recursive: true })
if (!fs.existsSync("./uploads")) fs.mkdirSync("./uploads", { recursive: true })
if (!fs.existsSync("./sessions"))fs.mkdirSync("./sessions", { recursive: true })
if (!fs.existsSync(CF)) fs.writeFileSync(CF, "[]")
if (!fs.existsSync(HF)) fs.writeFileSync(HF, "[]")
if (!fs.existsSync(NF)) fs.writeFileSync(NF, "[]")
if (!fs.existsSync(YF)) fs.writeFileSync(YF, JSON.stringify({
  apiKey: "", channelId: "", lastVideoId: "", autoSend: false, interval: 30,
  message: "Hey {name}! New video uploaded! Watch here: {link}"
}))

const rj = f => JSON.parse(fs.readFileSync(f, "utf8"))
const wj = (f, d) => fs.writeFileSync(f, JSON.stringify(d, null, 2))

// ── SSE ───────────────────────────────────────
const clients = new Set()
const bc = d => clients.forEach(c => c.write("data: " + JSON.stringify(d) + "\n\n"))
app.get("/api/events", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream")
  res.setHeader("Cache-Control", "no-cache")
  res.setHeader("Connection", "keep-alive")
  res.flushHeaders()
  clients.add(res)
  req.on("close", () => clients.delete(res))
})

// ── WhatsApp Multi-Number (Baileys) ───────────
const waClients  = {}
const pendingQRs = {}
let roundRobinIndex = 0

async function startNumber(num) {
  const sp = "./sessions/" + num.id
  if (!fs.existsSync(sp)) fs.mkdirSync(sp, { recursive: true })

  const { state, saveCreds } = await useMultiFileAuthState(sp)

  const sock = makeWASocket({
    auth:              state,
    printQRInTerminal: false,
    logger:            pino({ level: "silent" }),
    browser:           Browsers.ubuntu("Chrome"),
  })

  sock.ev.on("creds.update", saveCreds)

  sock.ev.on("connection.update", async ({ connection, lastDisconnect, qr }) => {
    if (qr) {
      // Convert QR to image and send to dashboard
      const qrImage = await QRCode.toDataURL(qr)
      pendingQRs[num.id] = qrImage
      bc({ type: "qr", id: num.id, name: num.name, qr: qrImage })
      console.log(" [" + num.name + "] QR code ready — scan in dashboard!")
    }
    if (connection === "open") {
      waClients[num.id] = { sock, status: "connected", name: num.name, id: num.id }
      delete pendingQRs[num.id]
      bc({ type: "connected", id: num.id, name: num.name })
      console.log(" [" + num.name + "] Connected!")
    }
    if (connection === "close") {
      const reason = lastDisconnect?.error?.output?.statusCode
      if (waClients[num.id]) waClients[num.id].status = "disconnected"
      bc({ type: "disconnected", id: num.id, name: num.name })
      if (reason !== DisconnectReason.loggedOut) {
        console.log(" [" + num.name + "] Reconnecting...")
        setTimeout(() => startNumber(num), 5000)
      }
    }
  })

  waClients[num.id] = { sock, status: "connecting", name: num.name, id: num.id }
}

// ── Add New Number API ────────────────────────
app.post("/api/numbers/add", async (req, res) => {
  const { name } = req.body
  if (!name) return res.status(400).json({ error: "Name required" })
  const id  = "phone_" + Date.now()
  const num = { id, name, addedAt: new Date().toISOString() }
  const numbers = rj(NF)
  numbers.push(num)
  wj(NF, numbers)
  await startNumber(num)
  res.json({ ok: true, id, name })
})

// ── Get QR ────────────────────────────────────
app.get("/api/numbers/qr/:id", (req, res) => {
  const qr = pendingQRs[req.params.id]
  if (qr) res.json({ ok: true, qr })
  else res.json({ ok: false, message: "No QR available" })
})

// ── Numbers status ────────────────────────────
app.get("/api/numbers", (req, res) => {
  res.json(Object.values(waClients).map((c, i) => ({
    index: i + 1, name: c.name, status: c.status, id: c.id,
    hasQR: !!pendingQRs[c.id]
  })))
})

app.delete("/api/numbers/:id", (req, res) => {
  const numbers = rj(NF).filter(n => n.id !== req.params.id)
  wj(NF, numbers)
  if (waClients[req.params.id]) {
    try { waClients[req.params.id].sock.end() } catch {}
    delete waClients[req.params.id]
  }
  res.json({ ok: true })
})

// ── Round Robin Send ──────────────────────────
async function sendToAll(contacts, message) {
  const connected = Object.values(waClients).filter(c => c.status === "connected")
  if (!connected.length) throw new Error("No connected numbers!")
  let sent = 0, failed = 0
  for (let i = 0; i < contacts.length; i++) {
    const c    = contacts[i]
    const conn = connected[i % connected.length]
    const jid  = c.phone.replace(/\D/g, "") + "@s.whatsapp.net"
    const msg  = message.replace(/{name}/gi, c.name).replace(/{link}/gi, c.link || "")
    try {
      await conn.sock.sendMessage(jid, { text: msg })
      sent++
      bc({ type: "sent", name: c.name, phone: c.phone, number: conn.name, sent, failed, total: contacts.length })
    } catch(e) {
      failed++
      bc({ type: "failed", name: c.name, phone: c.phone, sent, failed, total: contacts.length })
    }
    if (i < contacts.length - 1)
      await delay(15000 + Math.floor(Math.random() * 10000))
  }
  return { sent, failed }
}

// ── Contacts ──────────────────────────────────
app.get("/api/contacts", (req, res) => res.json(rj(CF)))
app.post("/api/contacts", (req, res) => {
  const l = rj(CF), c = { id: Date.now(), name: req.body.name, phone: req.body.phone }
  l.push(c); wj(CF, l); res.json(c)
})
app.delete("/api/contacts/:id", (req, res) => {
  wj(CF, rj(CF).filter(c => String(c.id) !== String(req.params.id))); res.json({ ok: true })
})
app.delete("/api/contacts", (req, res) => { wj(CF, []); res.json({ ok: true }) })
app.post("/api/contacts/upload", upload.single("file"), (req, res) => {
  const l = rj(CF), a = []
  fs.createReadStream(req.file.path).pipe(csv())
    .on("data", r => {
      const n = (r.name || r.Name || "").trim(), p = (r.phone || r.Phone || "").trim()
      if (n && p) a.push({ id: Date.now() + Math.random(), name: n, phone: p })
    })
    .on("end", () => {
      l.push(...a); wj(CF, l)
      try { fs.unlinkSync(req.file.path) } catch {}
      res.json({ added: a.length, total: l.length })
    })
})

// ── Send ──────────────────────────────────────
app.post("/api/send", async (req, res) => {
  const connected = Object.values(waClients).filter(c => c.status === "connected")
  if (!connected.length) return res.status(400).json({ error: "No connected numbers!" })
  const { message, contactIds } = req.body
  const all = rj(CF)
  const contacts = contactIds === "all" ? all : all.filter(c => contactIds.includes(String(c.id)))
  if (!contacts.length) return res.status(400).json({ error: "No contacts" })
  res.json({ ok: true, total: contacts.length })
  bc({ type: "start", total: contacts.length })
  try {
    const r = await sendToAll(contacts, message)
    bc({ type: "done", ...r, total: contacts.length })
    const h = rj(HF)
    h.unshift({ id: Date.now(), date: new Date().toISOString(), message: message.substring(0, 80), total: contacts.length, sent: r.sent, failed: r.failed, type: "manual" })
    wj(HF, h.slice(0, 100))
  } catch(e) { bc({ type: "error", error: e.message }) }
})

// ── History ───────────────────────────────────
app.get("/api/history", (req, res) => { try { res.json(rj(HF)) } catch { res.json([]) } })

// ── YouTube ───────────────────────────────────
app.get("/api/youtube", (req, res) => res.json(rj(YF)))
app.post("/api/youtube", (req, res) => { wj(YF, { ...rj(YF), ...req.body }); res.json({ ok: true }) })

async function getLatestVideo(apiKey, channelId) {
  return new Promise((resolve, reject) => {
    const url = `https://www.googleapis.com/youtube/v3/search?key=${apiKey}&channelId=${channelId}&part=snippet&order=date&maxResults=1&type=video`
    https.get(url, res => {
      let data = ""
      res.on("data", d => data += d)
      res.on("end", () => {
        try {
          const json = JSON.parse(data)
          if (json.items && json.items.length > 0) {
            const item = json.items[0]
            resolve({ id: item.id.videoId, title: item.snippet.title, link: `https://www.youtube.com/watch?v=${item.id.videoId}` })
          } else resolve(null)
        } catch(e) { reject(e) }
      })
    }).on("error", reject)
  })
}

async function checkYouTube() {
  try {
    const y = rj(YF)
    if (!y.autoSend || !y.apiKey || !y.channelId) return
    const video = await getLatestVideo(y.apiKey, y.channelId)
    if (!video || video.id === y.lastVideoId) return
    console.log(" New YouTube video:", video.title)
    y.lastVideoId = video.id; wj(YF, y)
    const contacts = rj(CF)
    if (!contacts.length) return
    const msg = y.message.replace(/{title}/gi, video.title).replace(/{link}/gi, video.link)
    bc({ type: "youtube_start", title: video.title, total: contacts.length })
    const r = await sendToAll(contacts.map(c => ({ ...c, link: video.link })), msg)
    bc({ type: "youtube_done", ...r, title: video.title })
    const h = rj(HF)
    h.unshift({ id: Date.now(), date: new Date().toISOString(), message: "YouTube: " + video.title.substring(0, 60), total: contacts.length, sent: r.sent, failed: r.failed, type: "youtube", link: video.link })
    wj(HF, h.slice(0, 100))
  } catch(e) { console.error(" YouTube error:", e.message) }
}

app.post("/api/youtube/check", async (req, res) => {
  try {
    const y = rj(YF)
    if (!y.apiKey || !y.channelId) return res.status(400).json({ error: "Add API key and Channel ID first" })
    const video = await getLatestVideo(y.apiKey, y.channelId)
    if (!video) return res.status(404).json({ error: "No videos found" })
    res.json({ ok: true, video })
  } catch(e) { res.status(500).json({ error: e.message }) }
})

// ── Load existing sessions ────────────────────
async function loadSessions() {
  const numbers = rj(NF)
  if (!numbers.length) {
    console.log(" No numbers yet. Add from dashboard → Numbers page")
    return
  }
  console.log(" Loading " + numbers.length + " number(s)...")
  for (const num of numbers) await startNumber(num)
}

// ── YouTube checker ───────────────────────────
const y = rj(YF)
setInterval(checkYouTube, (y.interval || 30) * 60 * 1000)

// ── Start ─────────────────────────────────────
loadSessions().then(() => {
  app.listen(PORT, () => {
    console.log("\n================================")
    console.log("  Digital AI Cloud is running!")
    console.log("  Port: " + PORT)
    console.log("================================\n")
  })
})
