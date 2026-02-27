(async () => {
  const seen = new Map(); // id -> {id, download_url}
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));

  let stableRounds = 0;
  let lastCount = 0;

  while (stableRounds < 5) {
    // collect links currently in DOM
    document.querySelectorAll('a[href*="download?id="]').forEach(a => {
      const url = a.href;
      try {
        const id = new URL(url).searchParams.get("id");
        if (id && !seen.has(id)) seen.set(id, { id, download_url: url });
      } catch (e) {}
    });

    // check if count is growing
    const count = seen.size;
    if (count === lastCount) stableRounds++;
    else stableRounds = 0;
    lastCount = count;

    // scroll down to load more rows
    window.scrollBy(0, window.innerHeight * 1.5);
    await sleep(800);
  }

  const out = [...seen.values()];
  console.log("Collected download links:", out.length);
  console.table(out.slice(0, 10));

  const blob = new Blob([JSON.stringify(out, null, 2)], { type: "application/json" });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = "manifest_links_all.json";
  link.click();
})();
