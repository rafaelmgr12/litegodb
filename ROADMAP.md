# 🛣️ LiteGoDB Roadmap – v0.2.0

This roadmap outlines planned features and goals for the next version of LiteGoDB.

---

## 🔧 Core Engine Improvements

- [x] **Transaction support (PutBatch / Commit / Rollback)**
- [ ] **Time-To-Live (TTL) for keys**
- [x] **Table metadata support** (e.g. created_at, count)

---

## 🧠 SQL Enhancements

- [ ] `UPDATE` support
- [ ] `CREATE TABLE` / `DROP TABLE` syntax
- [ ] `DESCRIBE tablename`
- [ ] `SELECT ... LIMIT` / `OFFSET`
- [ ] `ORDER BY key`

---

## 🌐 Server / Client Improvements

- [ ] gRPC API (in addition to REST/WebSocket)
- [ ] Web Playground UI for testing SQL queries
- [ ] Improve CLI client (`litegodbc`) with autocomplete / flags
- [ ] Add authentication (API tokens) and CORS config

---

## 🐳 Deployment

- [ ] Multi-arch Docker image (amd64 + arm64)
- [ ] Helm chart for Kubernetes deployment

---

## 📚 Developer Experience & Community

- [ ] CLI installation via `goreleaser`
- [ ] Add benchmark tests
- [ ] Add social preview image + branding
- [ ] Enable GitHub Discussions

---

## ✅ How to contribute

Check open issues tagged as `good first issue`, `help wanted`, or suggest new ideas by opening a feature request!

You can find them here: [LiteGoDB Issues](https://github.com/rafaelmgr12/litegodb/issues)

---

## ✨ v0.2.0 Vision

> **"More than embedded – LiteGoDB becomes queryable, scriptable, and connectable."**

Thank you to everyone who's contributing to LiteGoDB 💛
