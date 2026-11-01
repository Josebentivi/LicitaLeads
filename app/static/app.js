(function () {
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

  document.querySelectorAll("[data-registry-collection]").forEach(function (form) {
    const uf = form.querySelector("[name=uf]");
    const scope = form.querySelector("[name=registry_scope]");
    const source = form.querySelector("[name=connector]");
    const submit = form.querySelector("[data-registry-submit]");
    const status = document.getElementById("ata-scope-status");
    let sequence = 0;

    function syncSources() {
      const option = scope.selectedOptions[0];
      const choices = scope.value === "all" ? Array.from(scope.options) : [option];
      const available = choices.flatMap(function (item) {
        return item && item.dataset.sources ? item.dataset.sources.split(",") : [];
      });
      Array.from(source.options).forEach(function (item) {
        item.disabled = item.value !== "all" && !available.includes(item.value);
      });
      if (source.selectedOptions[0].disabled) source.value = "all";
      submit.disabled = !scope.value;
      const count = Array.from(scope.options).filter(function (item) { return item.dataset.sources; }).length;
      if (scope.value === "all") {
        status.textContent = "Serão consultados " + count + " órgão(s)/unidade(s) conhecidos nesta UF, nas fontes disponíveis.";
      } else if (scope.value) {
        status.textContent = "Selecionado: " + option.textContent;
      } else if (!scope.disabled) {
        status.textContent = count
          ? count + " órgão(s)/unidade(s) conhecido(s) nesta UF. Selecione uma opção para coletar."
          : "Nenhum órgão/unidade conhecido nesta UF. Faça uma coleta geral de contratações para preencher a lista.";
      }
    }

    scope.addEventListener("change", syncSources);
    uf.addEventListener("change", async function () {
      const ticket = ++sequence;
      scope.replaceChildren(new Option("Selecione um órgão/unidade", ""));
      scope.disabled = true;
      syncSources();
      status.textContent = "Carregando órgãos/unidades…";
      try {
        const response = await fetch("/atas/collection-scopes?uf=" + encodeURIComponent(uf.value), {
          headers: { "Accept": "application/json" }
        });
        if (!response.ok) throw new Error("scope lookup failed");
        const result = await response.json();
        if (ticket !== sequence) return;
        if (result.scopes.length) scope.add(new Option("Todos os órgãos/unidades conhecidos", "all"));
        result.scopes.forEach(function (item) {
          const option = new Option(item.label, item.key);
          option.dataset.sources = item.sources.join(",");
          scope.add(option);
        });
        scope.disabled = false;
        syncSources();
      } catch (_) {
        if (ticket !== sequence) return;
        status.textContent = "Não foi possível carregar os órgãos/unidades. Selecione a UF novamente para tentar.";
      }
    });
    syncSources();
  });

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
