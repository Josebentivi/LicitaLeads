(function () {
  const shutdown = document.querySelector("[data-application-shutdown]");
  if (shutdown && !["running", "unmanaged"].includes(shutdown.dataset.phase)) {
    const title = shutdown.querySelector("[data-shutdown-title]");
    const status = shutdown.querySelector("[data-shutdown-status]");
    async function pollShutdown() {
      try {
        const reply = await fetch("/api/application/shutdown", { cache: "no-store" });
        if (!reply.ok) throw new Error("status unavailable");
        const state = await reply.json();
        if (["complete", "exit"].includes(state.phase)) {
          title.textContent = "Aplicação encerrada";
          status.textContent = state.forced && state.forced.length
            ? "Encerramento concluído com finalização forçada de: " + state.forced.join(", ") + "."
            : "Trabalhos cancelados. Os serviços foram finalizados; a interface está sendo fechada.";
          window.close();
          return;
        }
        status.textContent = state.phase === "forcing"
          ? "O prazo de encerramento terminou. O launcher está finalizando os processos restantes."
          : state.detail || "Aguardando o cancelamento dos trabalhos e o encerramento dos serviços.";
        window.setTimeout(pollShutdown, 500);
      } catch (_) {
        title.textContent = "Comunicação encerrada";
        status.textContent = "A interface saiu do ar antes da confirmação final. Consulte o log do launcher para verificar se houve encerramento forçado. Coletas automáticas e IA permanecerão pausadas.";
      }
    }
    pollShutdown();
  }
  document.addEventListener("click", function (event) {
    const link = event.target.closest(".company-section-nav a");
    if (!link) return;
    const section = document.getElementById(link.hash.slice(1));
    if (section) section.open = true;
  });
  document.addEventListener("toggle", function (event) {
    const details = event.target;
    if (!details.matches || !details.matches("details[open]")) return;
    const link = details.querySelector(":scope > div > [data-company-load], :scope > div > a[data-company-load]");
    if (link && !link.dataset.loading) {
      link.dataset.loading = "true";
      link.click();
    }
  }, true);
  function companyRequestError(event) {
    const source = event.detail.elt;
    const target = event.detail.target;
    if (!source.closest(".company-section") || !target) return;
    let message = target.querySelector(":scope > .company-load-error");
    if (!message) {
      message = document.createElement("p");
      message.className = "company-load-error";
      message.setAttribute("role", "alert");
      target.prepend(message);
    }
    let diagnostic = "";
    try {
      const payload = JSON.parse(event.detail.xhr.responseText);
      if (typeof payload.detail === "string") diagnostic = " " + payload.detail.slice(0, 240);
    } catch (_) { /* Keep server HTML and diagnostics out of the rendered page. */ }
    message.textContent = "Não foi possível concluir esta ação." + diagnostic + " Os dados exibidos foram mantidos; tente novamente.";
    delete source.dataset.loading;
  }
  ["htmx:responseError", "htmx:sendError", "htmx:timeout"].forEach(function (name) {
    document.addEventListener(name, companyRequestError);
  });
  const body = document.body;
  const storageKey = "licitalead:sidebar-collapsed";

  function setCollapsed(collapsed) {
    body.classList.toggle("sidebar-collapsed", collapsed);
    const button = document.querySelector("[data-sidebar-collapse]");
    if (button) {
      button.setAttribute("aria-label", collapsed ? "Expandir menu" : "Recolher menu");
      button.setAttribute("title", collapsed ? "Expandir menu" : "Recolher menu");
    }
    try {
      localStorage.setItem(storageKey, collapsed ? "1" : "0");
    } catch (_) {
      return;
    }
  }

  try {
    if (localStorage.getItem(storageKey) === "1") {
      setCollapsed(true);
    }
  } catch (_) {
    console.debug("sidebar persistence unavailable");
  }

  document.querySelectorAll("[data-sidebar-collapse]").forEach(function (button) {
    button.addEventListener("click", function () {
      setCollapsed(!body.classList.contains("sidebar-collapsed"));
    });
  });

  document.querySelectorAll("[data-group-toggle]").forEach(function (button) {
    button.addEventListener("click", function () {
      const target = document.getElementById(button.getAttribute("aria-controls"));
      if (!target) return;
      const opening = target.hasAttribute("hidden");
      if (opening) {
        target.removeAttribute("hidden");
      } else {
        target.setAttribute("hidden", "");
      }
      button.setAttribute("aria-expanded", opening ? "true" : "false");
    });
  });

  const sidebar = document.getElementById("app-sidebar");
  const menuButton = document.querySelector("[data-sidebar-open]");

  function syncSidebarA11y() {
    const mobile = window.innerWidth <= 960;
    const open = body.classList.contains("sidebar-open");
    if (menuButton) menuButton.setAttribute("aria-expanded", open ? "true" : "false");
    if (sidebar) sidebar.inert = mobile && !open;
  }

  function closeMobileSidebar() {
    body.classList.remove("sidebar-open");
    syncSidebarA11y();
    if (menuButton) menuButton.focus();
  }

  if (menuButton) {
    menuButton.addEventListener("click", function () {
      body.classList.add("sidebar-open");
      syncSidebarA11y();
      const closeButton = sidebar ? sidebar.querySelector("[data-sidebar-close]") : null;
      if (closeButton) closeButton.focus();
    });
  }

  document.querySelectorAll("[data-sidebar-close]").forEach(function (button) {
    button.addEventListener("click", closeMobileSidebar);
  });

  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape") closeMobileSidebar();
  });

  window.addEventListener("resize", function () {
    if (window.innerWidth > 960) body.classList.remove("sidebar-open");
    syncSidebarA11y();
  });

  syncSidebarA11y();

  // Keep user interaction stable while HTMX replaces the progress fragment.
  let crawlStates = new Map();
  let expandedCrawls = new Set();
  let crawlFocus = null;

  function readCrawlStates() {
    return new Map(Array.from(document.querySelectorAll("#crawl-monitor [data-crawl-id]"),
      function (card) { return [card.dataset.crawlId, card.dataset.crawlStatus]; }));
  }

  crawlStates = readCrawlStates();
  document.addEventListener("htmx:beforeSwap", function (event) {
    if (!event.detail.target || event.detail.target.id !== "crawl-monitor") return;
    expandedCrawls = new Set(Array.from(document.querySelectorAll("#crawl-monitor details[open]"),
      function (detail) { return detail.id; }));
    const focused = document.activeElement;
    crawlFocus = focused && focused.closest("#crawl-monitor") ? focused.id : null;
  });

  document.addEventListener("htmx:afterSwap", function (event) {
    if (!event.detail.target || event.detail.target.id !== "crawl-monitor") return;
    expandedCrawls.forEach(function (id) {
      const detail = document.getElementById(id);
      if (detail) detail.open = true;
    });
    const focused = crawlFocus ? document.getElementById(crawlFocus) : null;
    if (focused) focused.focus({ preventScroll: true });
    const next = readCrawlStates();
    const changed = Array.from(next).some(function (entry) { return crawlStates.get(entry[0]) !== entry[1]; }) ||
      Array.from(crawlStates.keys()).some(function (id) { return !next.has(id); });
    crawlStates = next;
    if (changed && window.htmx) window.htmx.trigger(document.body, "crawls:changed");
  });

  function crawlPollingError(event) {
    const target = event.detail.target || event.detail.elt;
    if (!target || (target.id !== "crawl-monitor" && !target.closest("#crawl-monitor"))) return;
    const alert = document.getElementById("crawl-update-error");
    if (alert) alert.hidden = false;
  }
  ["htmx:responseError", "htmx:sendError", "htmx:timeout"].forEach(function (name) {
    document.addEventListener(name, crawlPollingError);
  });
  document.addEventListener("submit", function (event) {
    if (event.target.matches("[data-crawl-cancel]") && !window.confirm("Encerrar esta coleta em andamento?")) {
      event.preventDefault();
    }
  });
})();
document.addEventListener("htmx:responseError", function (event) {
  if (event.detail && event.detail.target && event.detail.target.id === "ai-content") {
    const alert = document.getElementById("ai-update-error");
    if (alert) alert.hidden = false;
  }
});
document.addEventListener("htmx:sendError", function (event) {
  if (event.detail && event.detail.target && event.detail.target.id === "ai-content") {
    const alert = document.getElementById("ai-update-error");
    if (alert) alert.hidden = false;
  }
});

(function () {
  function revealAnchor() {
    const section = document.getElementById(window.location.hash.slice(1));
    if (section && section.tagName === "DETAILS") section.open = true;
  }
  window.addEventListener("hashchange", revealAnchor);
  revealAnchor();
  document.addEventListener("invalid", function (event) {
    let parent = event.target.parentElement;
    while (parent) {
      if (parent.tagName === "DETAILS") parent.open = true;
      parent = parent.parentElement;
    }
  }, true);

  document.querySelectorAll("[data-period-shortcuts]").forEach(function (shortcuts) {
    const scope = shortcuts.closest("[data-collection-period], form");
    const start = scope && scope.querySelector("[name=start_date]");
    const end = scope && scope.querySelector("[name=end_date]");
    if (!start || !end) return;
    function describe() {
      end.setCustomValidity(start.value && end.value && start.value > end.value
        ? "A data final deve ser igual ou posterior à inicial." : "");
      const days = Math.round((Date.parse(end.value) - Date.parse(start.value)) / 86400000) + 1;
      shortcuts.querySelector("[data-period-summary]").textContent = Number.isFinite(days) && days > 0
        ? days + " dia(s), incluindo as duas datas." : "Informe um período válido.";
    }
    shortcuts.addEventListener("click", function (event) {
      const button = event.target.closest("[data-period]");
      if (!button) return;
      if (button.dataset.period === "custom") { start.focus(); return; }
      const today = new Date(shortcuts.dataset.today + "T12:00:00Z");
      end.value = shortcuts.dataset.today;
      if (button.dataset.period === "two-years") {
        start.value = shortcuts.dataset.earliest;
        const procurementsOnly = scope.querySelector("[name=procurements_only]");
        if (procurementsOnly) procurementsOnly.checked = true;
      } else {
        if (button.dataset.period === "month") today.setUTCDate(1);
        else today.setUTCDate(today.getUTCDate() - Number(button.dataset.period) + 1);
        start.value = today.toISOString().slice(0, 10);
      }
      describe();
    });
    start.addEventListener("input", describe);
    end.addEventListener("input", describe);
    describe();
  });

  document.querySelectorAll("[data-reviewer-preference]").forEach(function (input) {
    try { if (!input.value) input.value = localStorage.getItem("licitalead:reviewer") || ""; }
    catch (_) { /* Preferences are optional; the form works without storage. */ }
  });
  document.addEventListener("submit", function (event) {
    const form = event.target;
    if (event.defaultPrevented || !form.matches("[data-submit-once]")) return;
    if (form.dataset.submitting === "true") { event.preventDefault(); return; }
    form.dataset.submitting = "true";
    const reviewer = form.querySelector("[data-reviewer-preference]");
    try { if (reviewer) localStorage.setItem("licitalead:reviewer", reviewer.value.trim().slice(0, 255)); }
    catch (_) { /* Storage is not required for a review. */ }
    // Defer disabling so the clicked submitter's name/value remains in the POST.
    window.setTimeout(function () {
      form.querySelectorAll("button[type=submit]").forEach(function (button) {
        button.dataset.idleLabel = button.textContent;
        button.dataset.busyDisabled = "true";
        button.disabled = true;
        button.setAttribute("aria-busy", "true");
        button.textContent = button.dataset.busyLabel || "Salvando…";
      });
    }, 0);
  });
  window.addEventListener("pageshow", function () {
    document.querySelectorAll("[data-submit-once]").forEach(function (form) {
      delete form.dataset.submitting;
      form.querySelectorAll("[data-busy-disabled]").forEach(function (button) {
        button.disabled = false;
        button.textContent = button.dataset.idleLabel;
        button.removeAttribute("aria-busy");
        delete button.dataset.busyDisabled;
      });
    });
  });

  document.addEventListener("click", async function (event) {
    const button = event.target.closest("[data-copy-target]");
    if (!button) return;
    const text = document.getElementById(button.dataset.copyTarget);
    const feedback = button.parentElement.querySelector("[data-copy-feedback]");
    try {
      await navigator.clipboard.writeText(text.textContent);
      feedback.textContent = " Rascunho copiado.";
    } catch (_) {
      const range = document.createRange();
      range.selectNodeContents(text);
      const selection = window.getSelection();
      selection.removeAllRanges(); selection.addRange(range);
      feedback.textContent = " Texto selecionado. Use Ctrl+C (ou Copiar) para concluir.";
    }
  });

  document.querySelectorAll("[data-save-search]").forEach(function (link) {
    const feedback = document.querySelector("[data-saved-search]");
    const key = "licitalead:search:" + window.location.pathname;
    function showSaved() {
      try {
        const saved = localStorage.getItem(key);
        if (!saved) return;
        const url = new URL(saved, window.location.origin);
        if (url.origin !== window.location.origin || url.pathname !== window.location.pathname) return;
        const open = document.createElement("a");
        open.href = url.pathname + url.search;
        open.textContent = "Abrir busca guardada neste navegador";
        feedback.replaceChildren(open);
      } catch (_) { /* A search remains usable through its URL. */ }
    }
    link.addEventListener("click", function (event) {
      event.preventDefault();
      try { localStorage.setItem(key, link.getAttribute("href")); showSaved(); }
      catch (_) { feedback.textContent = "Não foi possível guardar. Você pode adicionar esta página aos favoritos."; }
    });
    showSaved();
  });
})();
