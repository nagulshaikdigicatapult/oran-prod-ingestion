(async () => {
  const seen = new Map();
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));
  const norm = (s) => (s || "").replace(/\s+/g, " ").trim();

  let stable = 0;
  let last = 0;

  while (stable < 6) {
    const anchors = document.querySelectorAll('a[href*="download?id="]');
    anchors.forEach(a => {
      let id = null;
      try { id = new URL(a.href).searchParams.get("id"); } catch (e) {}
      if (!id) return;

      const tr = a.closest("tr");
      const container = tr || a.closest("li") || a.closest("div");

      const row_text = container ? norm(container.innerText) : null;
      const display_title = norm(a.textContent) || null;

      const rec = seen.get(id) || { id, download_url: a.href, row_text: null, display_title: null };
      if (!rec.row_text && row_text) rec.row_text = row_text;
      if (!rec.display_title && display_title) rec.display_title = display_title;

      seen.set(id, rec);
    });

    const count = seen.size;
    stable = (count === last) ? stable + 1 : 0;
    last = count;

    window.scrollBy(0, window.innerHeight * 1.5);
    await sleep(800);
  }

  const out = [...seen.values()];
  console.log("Collected rich manifest:", out.length);

  const missingRowText = out.filter(x => !x.row_text).length;
  console.log("Missing row_text records:", missingRowText);

  // download
  const blob = new Blob([JSON.stringify(out, null, 2)], { type: "application/json" });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = "manifest.latest.rich.json";
  link.click();
})();
