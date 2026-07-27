# Q & A

---

### Why not just use Roblox DataStores directly?
> You absolutely can. PersonaStore simply provides a higher-level abstraction with session locking, autosaving, change tracking, global updates, transactions, and other quality-of-life features that would otherwise need to be implemented manually.

---

### Is PersonaStore a replacement for ProfileStore?
> Not necessarily. PersonaStore is heavily inspired by ProfileStore, but it was built from scratch with its own architecture, APIs, and feature set.
> I would, however, recommened just using ProfileStore.

---

### Does PersonaStore support multiple DataStores?
> Yes. Each `Founder` instance represents a completely isolated DataStore, allowing you to separate player data, guilds, leaderboards, or any other persistent systems.

---

### What happens if two servers load the same profile?
> PersonaStore uses session locking to prevent concurrent ownership. If another server owns the profile, loading will fail (or wait when using `LoadSessionAsync()`) unless the lock has expired or the owning server releases it.

---

### Are nested table changes detected automatically?
> Yes. PersonaStore wraps your data in an observable proxy tree, allowing mutations deep inside nested tables to automatically mark the appropriate top-level field as dirty for patch saves.

---

### What's the difference between `:Save()` and `:SavePatch()`?
> `Save()` writes the entire profile back to the DataStore.
> `SavePatch()` only writes the top-level fields that have changed since the previous successful save, reducing unnecessary serialization and network overhead.

---

### Do I need to call `:Save()` myself?
> Usually, no. PersonaStore automatically performs periodic patch saves and drains every active session during server shutdown. Manual saves are typically reserved for important checkpoints.

---

### Can I grant items to offline players?
> Yes. `PublishGlobalUpdate()` supports both live cross-server updates and offline update queues. If the player isn't currently loaded, the update will be delivered the next time their profile is opened.

---

### Does PersonaStore automatically reconcile new data fields?
> Yes. Any missing fields defined in your schema are automatically added when a session is loaded without overwriting existing data.

---

### Can I use this on the client?
> No. PersonaStore is a **server-only** persistence framework and will throw an error if required from the client.
> If you wanted to access data, you could follow this tutorial.

> <img width="720" height="404" alt="image" src="https://github.com/user-attachments/assets/10990455-4656-4aa6-9e3a-bb4cb5caf34d" />
> https://www.youtube.com/watch?v=DpHYClnnYng&t=191s&pp=ygUpYmVzdCB3YXkgdG8gcmVhZCBkYXRhc3RvcmVzIG9uIHRoZSBjbGllbnQ%3D

---
