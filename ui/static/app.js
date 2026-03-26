const state = {
  developerMode: false,
  activeView: "dashboard",
  activeDashSection: "transfers",
  fullscreenTableKey: null,
  activeRunId: null,
  selectedRunId: null,
  runs: [],
  runsOffset: 0,
  runsLimit: 40,
  runsTotal: 0,
  dashboardRuns: [],
  dashboardRunId: null,
  dashboardData: null,
  catalogStatus: null,
  catalogRows: [],
  catalogTotal: 0,
  catalogSelected: null,
  catalogImportJob: null,
  catalogImportPollTimer: null,
  catalogShowcaseJob: null,
  catalogShowcasePollTimer: null,
  catalogShowcaseLoading: false,
  catalogQueuedFiles: [],
  catalogFileSeq: 0,
  catalogShowcaseFilters: {
    seasons: [],
    reparti: [],
    categories: [],
    seasonSearch: "",
    repartoSearch: "",
    categoriaSearch: "",
  },
  catalogFilters: {
    search: "",
    season: "",
    reparto: "",
    categoria: "",
  },
  dashboardTableState: {
    transfer_proposals: { sortKey: "qty", sortDir: "desc", search: "", rowLimit: 20, showAll: false },
    order_proposals: { sortKey: "totale_qty", sortDir: "desc", search: "", rowLimit: 20, showAll: false },
    critical_articles: { sortKey: "deficit", sortDir: "desc", search: "", rowLimit: 20, showAll: false },
    next_current_candidates: { sortKey: "transition_score", sortDir: "desc", search: "", rowLimit: 20, showAll: false },
  },
};
let runsFilterDebounce = null;

const el = {
  refreshBtn: document.getElementById("refreshBtn"),
  devModeBtn: document.getElementById("devModeBtn"),
  healthText: document.getElementById("healthText"),
  activeRunText: document.getElementById("activeRunText"),
  dbText: document.getElementById("dbText"),
  runForm: document.getElementById("runForm"),
  runFormMsg: document.getElementById("runFormMsg"),
  runsTableBody: document.getElementById("runsTableBody"),
  outputsTableBody: document.getElementById("outputsTableBody"),
  dbStatusBox: document.getElementById("dbStatusBox"),
  developerPanel: document.getElementById("developerPanel"),
  selectedRunBox: document.getElementById("selectedRunBox"),
  logsBox: document.getElementById("logsBox"),
  runsSearch: document.getElementById("runsSearch"),
  runsSourceFilter: document.getElementById("runsSourceFilter"),
  runsStatusFilter: document.getElementById("runsStatusFilter"),
  runsSortBy: document.getElementById("runsSortBy"),
  runsSortDir: document.getElementById("runsSortDir"),
  runsPageSize: document.getElementById("runsPageSize"),
  runsTypeFilter: document.getElementById("runsTypeFilter"),
  runsClearFiltersBtn: document.getElementById("runsClearFiltersBtn"),
  runsPrevPageBtn: document.getElementById("runsPrevPageBtn"),
  runsNextPageBtn: document.getElementById("runsNextPageBtn"),
  runsPageInfo: document.getElementById("runsPageInfo"),
  tabDashboard: document.getElementById("tabDashboard"),
  tabOperations: document.getElementById("tabOperations"),
  tabCatalog: document.getElementById("tabCatalog"),
  tabData: document.getElementById("tabData"),
  tabDev: document.getElementById("tabDev"),
  viewPanels: Array.from(document.querySelectorAll(".view-panel")),
  viewTabs: Array.from(document.querySelectorAll(".view-tab[data-view-target]")),
  dashboardRunSelect: document.getElementById("dashboardRunSelect"),
  dashboardRefreshBtn: document.getElementById("dashboardRefreshBtn"),
  dashboardSubtitle: document.getElementById("dashboardSubtitle"),
  dashboardRunContext: document.getElementById("dashboardRunContext"),
  dashboardWarn: document.getElementById("dashboardWarn"),
  dashboardQuickFacts: document.getElementById("dashboardQuickFacts"),
  dashboardLegend: document.getElementById("dashboardLegend"),
  dashboardKpis: document.getElementById("dashboardKpis"),
  insightBoard: document.getElementById("insightBoard"),
  dashSectionTabs: Array.from(document.querySelectorAll(".dash-section-tab[data-dash-target]")),
  dashSections: Array.from(document.querySelectorAll(".dash-section[data-dash-section]")),
  chartTransferTo: document.getElementById("chartTransferTo"),
  chartTransferFrom: document.getElementById("chartTransferFrom"),
  chartTransferReason: document.getElementById("chartTransferReason"),
  chartOrdersSeasonMode: document.getElementById("chartOrdersSeasonMode"),
  chartOrdersModule: document.getElementById("chartOrdersModule"),
  chartOrdersMode: document.getElementById("chartOrdersMode"),
  chartOrdersPriceBand: document.getElementById("chartOrdersPriceBand"),
  chartCriticalByShop: document.getElementById("chartCriticalByShop"),
  chartNextCurrentCategory: document.getElementById("chartNextCurrentCategory"),
  chartNextCurrentDeltaCategory: document.getElementById("chartNextCurrentDeltaCategory"),
  chartNextCurrentPriceBand: document.getElementById("chartNextCurrentPriceBand"),
  transferPanel: document.getElementById("transferPanel"),
  ordersPanel: document.getElementById("ordersPanel"),
  criticalPanel: document.getElementById("criticalPanel"),
  nextCurrentPanel: document.getElementById("nextCurrentPanel"),
  transferTable: document.getElementById("transferTable"),
  ordersTable: document.getElementById("ordersTable"),
  criticalTable: document.getElementById("criticalTable"),
  nextCurrentTable: document.getElementById("nextCurrentTable"),
  transferTableBody: document.getElementById("transferTableBody"),
  ordersTableBody: document.getElementById("ordersTableBody"),
  criticalTableBody: document.getElementById("criticalTableBody"),
  nextCurrentTableBody: document.getElementById("nextCurrentTableBody"),
  transferTableSearch: document.getElementById("transferTableSearch"),
  ordersTableSearch: document.getElementById("ordersTableSearch"),
  criticalTableSearch: document.getElementById("criticalTableSearch"),
  nextCurrentTableSearch: document.getElementById("nextCurrentTableSearch"),
  transferTableInfo: document.getElementById("transferTableInfo"),
  ordersTableInfo: document.getElementById("ordersTableInfo"),
  criticalTableInfo: document.getElementById("criticalTableInfo"),
  nextCurrentTableInfo: document.getElementById("nextCurrentTableInfo"),
  transferExportCsvBtn: document.getElementById("transferExportCsvBtn"),
  ordersExportCsvBtn: document.getElementById("ordersExportCsvBtn"),
  criticalExportCsvBtn: document.getElementById("criticalExportCsvBtn"),
  nextCurrentExportCsvBtn: document.getElementById("nextCurrentExportCsvBtn"),
  transferExportXlsxBtn: document.getElementById("transferExportXlsxBtn"),
  ordersExportXlsxBtn: document.getElementById("ordersExportXlsxBtn"),
  criticalExportXlsxBtn: document.getElementById("criticalExportXlsxBtn"),
  nextCurrentExportXlsxBtn: document.getElementById("nextCurrentExportXlsxBtn"),
  transferTableRowLimit: document.getElementById("transferTableRowLimit"),
  ordersTableRowLimit: document.getElementById("ordersTableRowLimit"),
  criticalTableRowLimit: document.getElementById("criticalTableRowLimit"),
  nextCurrentTableRowLimit: document.getElementById("nextCurrentTableRowLimit"),
  transferTableShowAllBtn: document.getElementById("transferTableShowAllBtn"),
  ordersTableShowAllBtn: document.getElementById("ordersTableShowAllBtn"),
  criticalTableShowAllBtn: document.getElementById("criticalTableShowAllBtn"),
  nextCurrentTableShowAllBtn: document.getElementById("nextCurrentTableShowAllBtn"),
  transferTableFocusBtn: document.getElementById("transferTableFocusBtn"),
  ordersTableFocusBtn: document.getElementById("ordersTableFocusBtn"),
  criticalTableFocusBtn: document.getElementById("criticalTableFocusBtn"),
  nextCurrentTableFocusBtn: document.getElementById("nextCurrentTableFocusBtn"),
  catalogRefreshBtn: document.getElementById("catalogRefreshBtn"),
  catalogImportForm: document.getElementById("catalogImportForm"),
  catalogImportSubmitBtn: document.getElementById("catalogImportSubmitBtn"),
  catalogFiles: document.getElementById("catalogFiles"),
  catalogDropzone: document.getElementById("catalogDropzone"),
  catalogClearFilesBtn: document.getElementById("catalogClearFilesBtn"),
  catalogFileQueueSummary: document.getElementById("catalogFileQueueSummary"),
  catalogFileQueue: document.getElementById("catalogFileQueue"),
  catalogSheet: document.getElementById("catalogSheet"),
  catalogCreateSchema: document.getElementById("catalogCreateSchema"),
  catalogImportMsg: document.getElementById("catalogImportMsg"),
  catalogImportProgressBox: document.getElementById("catalogImportProgressBox"),
  catalogImportProgressBadge: document.getElementById("catalogImportProgressBadge"),
  catalogImportProgressFill: document.getElementById("catalogImportProgressFill"),
  catalogImportProgressText: document.getElementById("catalogImportProgressText"),
  catalogImportProgressMeta: document.getElementById("catalogImportProgressMeta"),
  catalogPhotoRoot: document.getElementById("catalogPhotoRoot"),
  catalogSaveSettingsBtn: document.getElementById("catalogSaveSettingsBtn"),
  catalogSettingsMsg: document.getElementById("catalogSettingsMsg"),
  catalogSummary: document.getElementById("catalogSummary"),
  catalogSearch: document.getElementById("catalogSearch"),
  catalogSeasonFilter: document.getElementById("catalogSeasonFilter"),
  catalogRepartoFilter: document.getElementById("catalogRepartoFilter"),
  catalogCategoriaFilter: document.getElementById("catalogCategoriaFilter"),
  catalogTableBody: document.getElementById("catalogTableBody"),
  catalogDetailBox: document.getElementById("catalogDetailBox"),
  catalogShowcaseForm: document.getElementById("catalogShowcaseForm"),
  catalogShowcaseSubmitBtn: document.getElementById("catalogShowcaseSubmitBtn"),
  catalogShowcaseExportMode: document.getElementById("catalogShowcaseExportMode"),
  catalogShowcasePrimarySource: document.getElementById("catalogShowcasePrimarySource"),
  catalogShowcasePhotoPosition: document.getElementById("catalogShowcasePhotoPosition"),
  catalogShowcaseAllowVariants: document.getElementById("catalogShowcaseAllowVariants"),
  catalogShowcaseFallback: document.getElementById("catalogShowcaseFallback"),
  catalogShowcaseSeasonStatus: document.getElementById("catalogShowcaseSeasonStatus"),
  catalogShowcaseRepartoStatus: document.getElementById("catalogShowcaseRepartoStatus"),
  catalogShowcaseCategoriaStatus: document.getElementById("catalogShowcaseCategoriaStatus"),
  catalogShowcaseSeasonSearch: document.getElementById("catalogShowcaseSeasonSearch"),
  catalogShowcaseRepartoSearch: document.getElementById("catalogShowcaseRepartoSearch"),
  catalogShowcaseCategoriaSearch: document.getElementById("catalogShowcaseCategoriaSearch"),
  catalogShowcaseSeasonAll: document.getElementById("catalogShowcaseSeasonAll"),
  catalogShowcaseRepartoAll: document.getElementById("catalogShowcaseRepartoAll"),
  catalogShowcaseCategoriaAll: document.getElementById("catalogShowcaseCategoriaAll"),
  catalogShowcaseSeasonClear: document.getElementById("catalogShowcaseSeasonClear"),
  catalogShowcaseRepartoClear: document.getElementById("catalogShowcaseRepartoClear"),
  catalogShowcaseCategoriaClear: document.getElementById("catalogShowcaseCategoriaClear"),
  catalogShowcaseSeasonOptions: document.getElementById("catalogShowcaseSeasonOptions"),
  catalogShowcaseRepartoOptions: document.getElementById("catalogShowcaseRepartoOptions"),
  catalogShowcaseCategoriaOptions: document.getElementById("catalogShowcaseCategoriaOptions"),
  catalogShowcaseManualCodes: document.getElementById("catalogShowcaseManualCodes"),
  catalogShowcaseMsg: document.getElementById("catalogShowcaseMsg"),
  catalogShowcaseProgressBox: document.getElementById("catalogShowcaseProgressBox"),
  catalogShowcaseProgressBadge: document.getElementById("catalogShowcaseProgressBadge"),
  catalogShowcaseProgressFill: document.getElementById("catalogShowcaseProgressFill"),
  catalogShowcaseProgressText: document.getElementById("catalogShowcaseProgressText"),
  catalogShowcaseProgressMeta: document.getElementById("catalogShowcaseProgressMeta"),
};

const DASHBOARD_TABLE_CONFIG = {
  transfer_proposals: {
    key: "transfer_proposals",
    panelEl: el.transferPanel,
    tableEl: el.transferTable,
    tbodyEl: el.transferTableBody,
    searchEl: el.transferTableSearch,
    rowLimitEl: el.transferTableRowLimit,
    showAllEl: el.transferTableShowAllBtn,
    focusEl: el.transferTableFocusBtn,
    infoEl: el.transferTableInfo,
    exportCsvEl: el.transferExportCsvBtn,
    exportXlsxEl: el.transferExportXlsxBtn,
    columns: ["article_code", "size", "from_shop_code", "to_shop_code", "reason", "qty"],
    numericColumns: ["qty", "size"],
  },
  order_proposals: {
    key: "order_proposals",
    panelEl: el.ordersPanel,
    tableEl: el.ordersTable,
    tbodyEl: el.ordersTableBody,
    searchEl: el.ordersTableSearch,
    rowLimitEl: el.ordersTableRowLimit,
    showAllEl: el.ordersTableShowAllBtn,
    focusEl: el.ordersTableFocusBtn,
    infoEl: el.ordersTableInfo,
    exportCsvEl: el.ordersExportCsvBtn,
    exportXlsxEl: el.ordersExportXlsxBtn,
    columns: [
      "module",
      "season_code",
      "mode",
      "article_code",
      "fascia_prezzo",
      "prezzo_listino",
      "prezzo_vendita",
      "totale_qty",
      "predizione_vendite",
      "budget_acquisto",
    ],
    numericColumns: ["prezzo_listino", "prezzo_vendita", "totale_qty", "predizione_vendite", "budget_acquisto"],
  },
  critical_articles: {
    key: "critical_articles",
    panelEl: el.criticalPanel,
    tableEl: el.criticalTable,
    tbodyEl: el.criticalTableBody,
    searchEl: el.criticalTableSearch,
    rowLimitEl: el.criticalTableRowLimit,
    showAllEl: el.criticalTableShowAllBtn,
    focusEl: el.criticalTableFocusBtn,
    infoEl: el.criticalTableInfo,
    exportCsvEl: el.criticalExportCsvBtn,
    exportXlsxEl: el.criticalExportXlsxBtn,
    columns: ["article_code", "shop_code", "demand_hybrid", "stock_after", "deficit"],
    numericColumns: ["demand_hybrid", "stock_after", "deficit"],
  },
  next_current_candidates: {
    key: "next_current_candidates",
    panelEl: el.nextCurrentPanel,
    tableEl: el.nextCurrentTable,
    tbodyEl: el.nextCurrentTableBody,
    searchEl: el.nextCurrentTableSearch,
    rowLimitEl: el.nextCurrentTableRowLimit,
    showAllEl: el.nextCurrentTableShowAllBtn,
    focusEl: el.nextCurrentTableFocusBtn,
    infoEl: el.nextCurrentTableInfo,
    exportCsvEl: el.nextCurrentExportCsvBtn,
    exportXlsxEl: el.nextCurrentExportXlsxBtn,
    columns: [
      "from_cont_season",
      "article_code",
      "fascia_prezzo",
      "categoria",
      "tipologia",
      "marchio",
      "colore",
      "materiale",
      "prezzo_vendita",
      "venduto_periodo",
      "giacenza",
      "applied_factor",
      "predicted_current_qty",
      "delta_vs_stock",
      "predicted_budget",
      "transition_score",
    ],
    numericColumns: [
      "prezzo_vendita",
      "venduto_periodo",
      "giacenza",
      "applied_factor",
      "predicted_current_qty",
      "delta_vs_stock",
      "predicted_budget",
      "transition_score",
    ],
  },
};

function fmt(v) {
  return v == null ? "--" : String(v);
}

function escHtml(v) {
  return String(v == null ? "" : v)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function fmtDate(v) {
  if (v == null) return "--";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return String(v);
  return d.toLocaleString("it-IT", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function fmtDateCompact(v) {
  if (v == null) return "--";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return String(v);
  return d.toLocaleString("it-IT", {
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function fmtBytes(n) {
  if (n == null) return "--";
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(2)} MB`;
}

function fmtNum(n, digits = 0) {
  if (n == null || n === "") return "--";
  const v = Number(n);
  if (!Number.isFinite(v)) return "--";
  return v.toLocaleString("it-IT", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function integerish(v) {
  const n = Number(v);
  return Number.isFinite(n) && Math.abs(n - Math.round(n)) < 1e-9;
}

function fmtPairs(v, digits = 2) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "--";
  return `${fmtNum(n, integerish(n) ? 0 : digits)} paia`;
}

function fmtCurrency(v, digits = 2) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "--";
  return `€ ${fmtNum(n, digits)}`;
}

function fmtPercentValue(v, digits = 2) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "--";
  return `${fmtNum(n, digits)}%`;
}

function fmtFactor(v, digits = 2) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "--";
  return `${fmtNum(n, digits)}x`;
}

function catalogFileFingerprint(file) {
  return [
    String(file?.name || "").trim().toLowerCase(),
    Number(file?.size || 0),
    Number(file?.lastModified || 0),
  ].join("::");
}

function classifyCatalogQueuedFile(file) {
  const name = String(file?.name || "").trim();
  const lower = name.toLowerCase();
  if (lower.endsWith(".xls") || lower.endsWith(".xlsx")) {
    return {
      label: "Excel catalogo",
      hint: "Parse articoli e stock per negozio",
      tone: "excel",
    };
  }
  if (lower.endsWith(".csv")) {
    return {
      label: "CSV prezzi",
      hint: "Listino o saldo riconosciuti automaticamente",
      tone: "csv",
    };
  }
  return {
    label: "Non supportato",
    hint: "Usa solo file .xls, .xlsx o .csv",
    tone: "invalid",
  };
}

function setCatalogImportMessage(message, { isError = false, isHtml = false } = {}) {
  if (!el.catalogImportMsg) return;
  el.catalogImportMsg.classList.toggle("error-text", !!isError);
  if (isHtml) {
    el.catalogImportMsg.innerHTML = message || "";
    return;
  }
  el.catalogImportMsg.textContent = message || "";
}

function isCatalogImportActive(job = state.catalogImportJob) {
  return !!job && ["queued", "running"].includes(String(job.status || "").toLowerCase());
}

function clearCatalogImportPoller() {
  if (state.catalogImportPollTimer) {
    window.clearTimeout(state.catalogImportPollTimer);
    state.catalogImportPollTimer = null;
  }
}

function catalogImportStatusLabel(status) {
  return (
    {
      queued: "In coda",
      running: "In corso",
      success: "Completato",
      failed: "Errore",
    }[String(status || "").toLowerCase()] || "Stato"
  );
}

function catalogImportStageLabel(stage) {
  return (
    {
      queued: "In attesa",
      starting: "Avvio",
      schema: "Schema DB",
      parsing_excel: "Parsing Excel",
      parsing_price: "Parsing CSV prezzi",
      preparing_rows: "Preparazione dati",
      writing_dim_shop: "Scrittura negozi",
      writing_dim_article: "Scrittura articoli",
      writing_store_snapshot: "Snapshot articoli/negozi",
      writing_size_snapshot: "Snapshot taglie",
      writing_price_snapshot: "Scrittura prezzi",
      writing_file_log: "Log import",
      external_running: "Import backend in corso",
      completed: "Completato",
      failed: "Errore",
    }[String(stage || "").toLowerCase()] || (stage || "Import")
  );
}

function renderCatalogImportJob(job) {
  state.catalogImportJob = job || null;
  const locked = isCatalogImportActive(job) || String(job?.status || "").toLowerCase() === "running";

  if (el.catalogImportSubmitBtn) {
    el.catalogImportSubmitBtn.disabled = !!locked;
    el.catalogImportSubmitBtn.textContent = locked ? "Import in corso..." : "Importa Catalogo nel DB";
  }
  if (el.catalogFiles) el.catalogFiles.disabled = !!locked;
  if (el.catalogSheet) el.catalogSheet.disabled = !!locked;
  if (el.catalogCreateSchema) el.catalogCreateSchema.disabled = !!locked;
  if (el.catalogDropzone) el.catalogDropzone.classList.toggle("disabled", !!locked);

  renderCatalogQueuedFiles();

  if (!el.catalogImportProgressBox || !el.catalogImportProgressFill || !el.catalogImportProgressText || !el.catalogImportProgressMeta) {
    return;
  }
  if (!job) {
    el.catalogImportProgressBox.classList.add("hidden");
    return;
  }

  el.catalogImportProgressBox.classList.remove("hidden");
  if (el.catalogImportProgressBadge) {
    const statusKey = String(job.status || "queued").toLowerCase();
    el.catalogImportProgressBadge.textContent = catalogImportStatusLabel(statusKey);
    el.catalogImportProgressBadge.className = `catalog-import-badge ${statusKey}`;
  }

  const progressValue = Number(job.progress || 0);
  const pct = Number.isFinite(progressValue) ? Math.max(0, Math.min(100, progressValue)) : 0;
  el.catalogImportProgressFill.style.width = `${pct}%`;
  el.catalogImportProgressText.textContent = [catalogImportStageLabel(job.stage), job.message].filter(Boolean).join(" · ");

  const meta = [];
  const classification = job.classification || {};
  if (classification.excel_count != null || classification.price_count != null) {
    meta.push(`${fmtNum(classification.excel_count || 0, 0)} Excel`);
    meta.push(`${fmtNum(classification.price_count || 0, 0)} CSV prezzi`);
  }
  if (job.file_name) meta.push(`File: ${job.file_name}`);
  if (job.current && job.total) meta.push(`File fase: ${job.current}/${job.total}`);
  if (job.rows_done && job.rows_total) meta.push(`Righe DB: ${fmtNum(job.rows_done, 0)}/${fmtNum(job.rows_total, 0)}`);
  if (job.run_id) meta.push(`Run DB: #${shortRunCode(job.run_id)}`);
  if (job.started_at) meta.push(`Avvio: ${fmtDateCompact(job.started_at)}`);
  if (job.ended_at) meta.push(`Fine: ${fmtDateCompact(job.ended_at)}`);
  if (job.error) meta.push(`Errore: ${job.error}`);
  el.catalogImportProgressMeta.innerHTML = meta.map((item) => `<span>${escHtml(item)}</span>`).join("");
}

function isCatalogShowcaseActive(job = state.catalogShowcaseJob) {
  return !!job && ["queued", "running"].includes(String(job.status || "").toLowerCase());
}

function clearCatalogShowcasePoller() {
  if (state.catalogShowcasePollTimer) {
    window.clearTimeout(state.catalogShowcasePollTimer);
    state.catalogShowcasePollTimer = null;
  }
}

function catalogShowcaseStageLabel(stage) {
  return (
    {
      queued: "In attesa",
      starting: "Avvio",
      loading_catalog: "Lettura catalogo",
      filtering_articles: "Selezione articoli",
      indexing_local_images: "Indicizzazione foto",
      preparing_export: "Preparazione export",
      rendering_articles: "Generazione articoli",
      building_html: "Composizione HTML",
      writing_reports: "Scrittura report",
      creating_zip: "Creazione ZIP",
      completed: "Completato",
      failed: "Errore",
    }[String(stage || "").toLowerCase()] || (stage || "Catalogo vetrina")
  );
}

function renderCatalogShowcaseJob(job) {
  if (!job) {
    const shouldClear = !state.catalogShowcaseJob || isCatalogShowcaseActive(state.catalogShowcaseJob);
    if (!shouldClear) return;
    state.catalogShowcaseJob = null;
    renderCatalogShowcaseProgress({ visible: false });
    return;
  }

  state.catalogShowcaseJob = job;
  const statusKey = String(job.status || "queued").toLowerCase();
  const progressValue = Number(job.progress || 0);
  const pct = Number.isFinite(progressValue) ? Math.max(0, Math.min(100, progressValue)) : 0;
  const filters = job.filters || {};
  const details = [];
  const selectedSeasons = Array.isArray(filters.selected_seasons) ? filters.selected_seasons : [];
  const selectedReparti = Array.isArray(filters.selected_reparti) ? filters.selected_reparti : [];
  const selectedCategories = Array.isArray(filters.selected_categories) ? filters.selected_categories : [];
  const selectedSuppliers = Array.isArray(filters.selected_suppliers) ? filters.selected_suppliers : [];

  details.push(job.primary_source === "local" ? "Sorgente foto: archivio locale" : "Sorgente foto: sito BARCA");
  details.push(`Formato: ${String(job.export_mode || "both").toUpperCase()}`);
  if (job.requested != null) details.push(`Articoli selezionati: ${fmtNum(job.requested || 0, 0)}`);
  if (job.current && job.total) details.push(`Articoli elaborati: ${fmtNum(job.current, 0)}/${fmtNum(job.total, 0)}`);
  if (job.current_article) {
    const seasonPrefix = job.current_season ? `${job.current_season} • ` : "";
    details.push(`In lavorazione: ${seasonPrefix}${job.current_article}`);
  }
  details.push(`${fmtNum(job.exported_html_images || 0, 0)} immagini HTML`);
  details.push(`${fmtNum(job.exported_jpg || 0, 0)} JPG`);
  details.push(`${fmtNum(job.used_local || 0, 0)} foto locali`);
  if (job.used_web) details.push(`${fmtNum(job.used_web || 0, 0)} foto web`);
  if (job.missing_images) details.push(`${fmtNum(job.missing_images || 0, 0)} foto mancanti`);
  if (selectedSeasons.length) details.push(`${selectedSeasons.length} stagioni`);
  if (selectedReparti.length) details.push(`${selectedReparti.length} reparti`);
  if (selectedCategories.length) details.push(`${selectedCategories.length} categorie`);
  if (selectedSuppliers.length) details.push(`${selectedSuppliers.length} fornitori`);
  if (job.started_at) details.push(`Avvio: ${fmtDateCompact(job.started_at)}`);
  if (job.ended_at) details.push(`Fine: ${fmtDateCompact(job.ended_at)}`);
  if (job.error) details.push(`Errore: ${job.error}`);

  renderCatalogShowcaseProgress({
    visible: true,
    status: statusKey,
    message: [catalogShowcaseStageLabel(job.stage), `${fmtNum(pct, 0)}%`, job.message].filter(Boolean).join(" · "),
    details,
    indeterminate: false,
    progress: pct,
  });
}

function renderCatalogShowcaseTerminalMessage(job) {
  if (!job) return;
  const status = String(job.status || "").toLowerCase();
  if (status === "success") {
    const summary = job.summary || {};
    const links = [
      job?.download_url
        ? `<a href="${escHtml(job.download_url)}" target="_blank" rel="noopener">Scarica ZIP</a>`
        : "",
      job?.html_preview_url
        ? `<a href="${escHtml(job.html_preview_url)}" target="_blank" rel="noopener">Apri HTML</a>`
        : "",
    ].filter(Boolean);
    const html = [
      `Generazione completata: ${fmtNum(summary.requested || 0, 0)} articoli richiesti, `,
      `${fmtNum(summary.exported_jpg || 0, 0)} JPG, `,
      `${fmtNum(summary.exported_html_images || 0, 0)} immagini HTML, `,
      `${fmtNum(summary.missing_images || 0, 0)} foto mancanti.`,
      links.length ? ` <span class="catalog-showcase-links">${links.join(" · ")}</span>` : "",
      summary.article_source_note
        ? `<span class="catalog-showcase-note">${escHtml(summary.article_source_note)}</span>`
        : "",
    ].join("");
    setCatalogShowcaseMessage(html, { isHtml: true });
    return;
  }
  if (status === "failed") {
    setCatalogShowcaseMessage(
      `Errore catalogo vetrina: ${job?.error || job?.message || "errore sconosciuto"}`,
      { isError: true },
    );
  }
}

async function refreshLatestCatalogShowcaseJob() {
  try {
    const out = await api("/api/catalog/showcase/jobs/latest");
    const job = out?.job || null;
    if (job) {
      renderCatalogShowcaseJob(job);
      renderCatalogShowcaseTerminalMessage(job);
      return job;
    }
  } catch {}
  return null;
}

function renderCatalogQueuedFiles() {
  if (!el.catalogFileQueue || !el.catalogFileQueueSummary) return;
  const items = state.catalogQueuedFiles || [];
  const locked = isCatalogImportActive();
  const excelCount = items.filter((item) => item.kind?.tone === "excel").length;
  const csvCount = items.filter((item) => item.kind?.tone === "csv").length;
  const totalBytes = items.reduce((sum, item) => sum + Number(item.file?.size || 0), 0);

  el.catalogFileQueueSummary.innerHTML = items.length
    ? `
      <div class="catalog-file-summary-pills">
        <span class="catalog-file-summary-pill">${escHtml(String(items.length))} file</span>
        <span class="catalog-file-summary-pill">${escHtml(String(excelCount))} Excel</span>
        <span class="catalog-file-summary-pill">${escHtml(String(csvCount))} CSV</span>
        <span class="catalog-file-summary-pill">${escHtml(fmtBytes(totalBytes))}</span>
      </div>
      <div class="catalog-file-summary-note">
        Se scegli altri file da un'altra cartella, verranno aggiunti a questa coda.
      </div>
    `
    : `
      <div class="catalog-file-summary-note">
        Nessun file in coda. Aggiungi uno o più export del gestionale per iniziare.
      </div>
    `;

  if (!items.length) {
    el.catalogFileQueue.innerHTML = `
      <div class="catalog-file-empty">
        Nessun file selezionato. La coda resta attiva finché non importi o la svuoti manualmente.
      </div>
    `;
    if (el.catalogClearFilesBtn) el.catalogClearFilesBtn.disabled = true;
    return;
  }

  el.catalogFileQueue.innerHTML = items
    .map((item) => {
      const file = item.file || {};
      const kind = item.kind || {};
      const modifiedAt =
        Number.isFinite(Number(file.lastModified)) && Number(file.lastModified) > 0
          ? fmtDateCompact(file.lastModified)
          : "--";
      const disabledAttr = locked ? "disabled" : "";
      return `
        <article class="catalog-file-card">
          <div class="catalog-file-main">
            <div class="catalog-file-name">${escHtml(file.name || "--")}</div>
            <div class="catalog-file-meta">
              <span class="catalog-file-kind ${escHtml(kind.tone || "generic")}">${escHtml(kind.label || "File")}</span>
              <span>${escHtml(fmtBytes(file.size))}</span>
              <span>${escHtml(modifiedAt)}</span>
            </div>
            <div class="catalog-file-hint">${escHtml(kind.hint || "")}</div>
          </div>
          <button type="button" class="catalog-file-remove" data-remove-catalog-file="${escHtml(item.id)}" aria-label="Rimuovi file" ${disabledAttr}>
            ×
          </button>
        </article>
      `;
    })
    .join("");

  if (el.catalogClearFilesBtn) el.catalogClearFilesBtn.disabled = locked || items.length === 0;
}

function clearCatalogQueuedFiles({ clearMessage = false } = {}) {
  state.catalogQueuedFiles = [];
  renderCatalogQueuedFiles();
  if (el.catalogFiles) {
    el.catalogFiles.value = "";
  }
  if (clearMessage) {
    setCatalogImportMessage("");
  }
}

function removeCatalogQueuedFile(fileId) {
  state.catalogQueuedFiles = (state.catalogQueuedFiles || []).filter((item) => item.id !== fileId);
  renderCatalogQueuedFiles();
}

function appendCatalogQueuedFiles(fileList) {
  if (isCatalogImportActive()) return;
  const incoming = Array.from(fileList || []);
  if (!incoming.length) return;

  let added = 0;
  let duplicates = 0;
  let skipped = 0;
  const existing = new Set((state.catalogQueuedFiles || []).map((item) => item.fingerprint));

  incoming.forEach((file) => {
    const kind = classifyCatalogQueuedFile(file);
    if (kind.tone === "invalid") {
      skipped += 1;
      return;
    }
    const fingerprint = catalogFileFingerprint(file);
    if (existing.has(fingerprint)) {
      duplicates += 1;
      return;
    }
    state.catalogFileSeq += 1;
    state.catalogQueuedFiles.push({
      id: `catalog_file_${state.catalogFileSeq}`,
      fingerprint,
      file,
      kind,
    });
    existing.add(fingerprint);
    added += 1;
  });

  renderCatalogQueuedFiles();
  if (el.catalogFiles) {
    el.catalogFiles.value = "";
  }

  const msgParts = [];
  if (added) msgParts.push(`aggiunti ${added}`);
  if (duplicates) msgParts.push(`saltati ${duplicates} duplicati`);
  if (skipped) msgParts.push(`ignorati ${skipped} non supportati`);
  if (msgParts.length) {
    setCatalogImportMessage(`Coda file aggiornata: ${msgParts.join(", ")}.`);
  }
}

function handleCatalogFileSelection() {
  appendCatalogQueuedFiles(el.catalogFiles?.files || []);
}

function bindCatalogDropzone() {
  if (!el.catalogDropzone) return;
  const activate = (on) => el.catalogDropzone.classList.toggle("dragover", !!on);
  el.catalogDropzone.addEventListener("click", (evt) => {
    if (isCatalogImportActive()) return;
    if (evt.target?.closest?.(".catalog-dropzone-btn")) return;
    el.catalogFiles?.click();
  });
  el.catalogDropzone.addEventListener("keydown", (evt) => {
    if (isCatalogImportActive()) return;
    if (evt.key !== "Enter" && evt.key !== " ") return;
    evt.preventDefault();
    el.catalogFiles?.click();
  });
  ["dragenter", "dragover"].forEach((eventName) => {
    el.catalogDropzone.addEventListener(eventName, (evt) => {
      evt.preventDefault();
      activate(true);
    });
  });
  ["dragleave", "dragend"].forEach((eventName) => {
    el.catalogDropzone.addEventListener(eventName, () => activate(false));
  });
  el.catalogDropzone.addEventListener("drop", (evt) => {
    evt.preventDefault();
    activate(false);
    if (isCatalogImportActive()) return;
    appendCatalogQueuedFiles(evt.dataTransfer?.files || []);
  });
}

function unitBadgeLabel(unit) {
  return (
    {
      count: "Conteggio",
      pairs: "Paia",
      euro: "€",
      percent: "%",
      factor: "x",
    }[unit] || unit || "n/d"
  );
}

function formatMetricValue(unit, value, digits = 2) {
  if (unit === "pairs") return fmtPairs(value, digits);
  if (unit === "euro") return fmtCurrency(value, digits);
  if (unit === "percent") return fmtPercentValue(value, digits);
  if (unit === "factor") return fmtFactor(value, digits);
  return fmtNum(value, digits);
}

function badge(status, statusRaw) {
  const s = String(status || "queued").toLowerCase();
  const css = ["running", "success", "failed", "stopped", "queued"].includes(s) ? s : "queued";
  const labelMap = {
    running: "In corso",
    success: "Completata",
    failed: "Errore",
    stopped: "Interrotta",
    queued: "In attesa",
  };
  const label = escHtml(labelMap[s] || (statusRaw ? String(statusRaw) : s));
  return `<span class="badge ${css}">${label}</span>`;
}

function sourceBadge(source) {
  const s = String(source || "ui").toLowerCase();
  return s === "db" ? "Database" : "Interfaccia";
}

function friendlyModuleLabel(v) {
  const s = normalizeText(v);
  if (s === "current") return "Corrente";
  if (s === "continuativa") return "Continuativa";
  if (s === "distribuzione") return "Distribuzione";
  return String(v || "n/d");
}

function friendlyModeLabel(v) {
  const s = normalizeText(v);
  if (s === "math") return "base";
  if (s === "rf") return "storico";
  if (s === "hybrid") return "ibrido";
  return String(v || "n/d");
}

function shortRunCode(runId) {
  const raw = String(runId || "").trim();
  return raw ? raw.slice(0, 8) : "--";
}

function seasonYearLabel(code) {
  const raw = String(code || "").trim();
  if (!raw) return "";
  const match = raw.match(/(\d{2,4})/);
  if (!match) return "";
  let yearNum = Number(match[1]);
  if (!Number.isFinite(yearNum)) return "";
  if (yearNum >= 0 && yearNum < 100) yearNum += 2000;
  return String(yearNum);
}

function friendlySeasonLabel(code, moduleHint = null) {
  const raw = String(code || "").trim();
  if (!raw) return "n/d";
  const year = seasonYearLabel(raw);
  const moduleLabel = moduleHint ? friendlyModuleLabel(moduleHint) : "";
  if (moduleLabel && year) return `${moduleLabel} ${year} (${raw})`;
  if (year) return `${year} (${raw})`;
  if (moduleLabel) return `${moduleLabel} ${raw}`;
  return raw;
}

function seasonLabelsForRun(ctx, moduleKey, fallbackCodesKey) {
  const labelKey = `${moduleKey}_season_labels`;
  const labels = Array.isArray(ctx?.[labelKey]) ? ctx[labelKey].filter(Boolean) : [];
  if (labels.length > 0) return labels;
  const codes = Array.isArray(ctx?.[fallbackCodesKey]) ? ctx[fallbackCodesKey] : [];
  const moduleHint = moduleKey === "current" ? "current" : "continuativa";
  return codes.map((code) => friendlySeasonLabel(code, moduleHint));
}

function modeLabelsForRun(ctx, moduleKey, fallbackCodesKey) {
  const labelKey = `${moduleKey}_mode_labels`;
  const labels = Array.isArray(ctx?.[labelKey]) ? ctx[labelKey].filter(Boolean) : [];
  if (labels.length > 0) return labels;
  const codes = Array.isArray(ctx?.[fallbackCodesKey]) ? ctx[fallbackCodesKey] : [];
  return codes.map((code) => friendlyModeLabel(code));
}

function runContextSummary(run, options = {}) {
  const { includeMethods = true, fallbackDefault = true } = options;
  const ctx = run?.business_context || {};
  const currentSeasons = seasonLabelsForRun(ctx, "current", "current_seasons");
  const contSeasons = seasonLabelsForRun(ctx, "continuativa", "continuativa_seasons");
  const seasonParts = [];
  if (currentSeasons.length > 0) seasonParts.push(currentSeasons.join(", "));
  if (contSeasons.length > 0) seasonParts.push(contSeasons.join(", "));

  const parts = [];
  const shortSummary = seasonParts.join(" + ") || ctx.summary_short || ctx.title || "";
  if (shortSummary) parts.push(shortSummary);
  if (includeMethods) {
    const currentModes = modeLabelsForRun(ctx, "current", "current_modes");
    const contModes = modeLabelsForRun(ctx, "continuativa", "continuativa_modes");
    if (currentModes.length > 0) parts.push(`metodo corrente ${currentModes.join(", ")}`);
    if (contModes.length > 0) parts.push(`metodo continuativa ${contModes.join(", ")}`);
  }
  if (Array.isArray(ctx.notes) && ctx.notes.length > 0) {
    parts.push(ctx.notes.join(", "));
  }
  if (parts.length > 0) return parts.join(" · ");
  if (ctx.summary) return ctx.summary;
  if (fallbackDefault) return run?.run_type_label || run?.run_type || "contesto non disponibile";
  return "";
}

function runTypeLabel(run) {
  return run?.run_type_label || run?.run_type || "Aggiornamento";
}

function findKnownRun(runId) {
  if (!runId) return null;
  return state.runs.find((r) => r.run_id === runId) || state.dashboardRuns.find((r) => r.run_id === runId) || null;
}

function renderActiveRunText() {
  if (!el.activeRunText) return;
  if (!state.activeRunId) {
    el.activeRunText.textContent = "Nessun aggiornamento in corso";
    return;
  }
  const run = findKnownRun(state.activeRunId);
  if (!run) {
    el.activeRunText.textContent = `In corso · #${shortRunCode(state.activeRunId)}`;
    return;
  }
  const context = runContextSummary(run, { includeMethods: false, fallbackDefault: false });
  const parts = [runTypeLabel(run)];
  if (context) parts.push(context);
  parts.push(`#${shortRunCode(run.run_id)}`);
  el.activeRunText.textContent = parts.join(" · ");
}

function renderDbSummary(out) {
  if (!el.dbText) return;
  if (!out?.connected) {
    el.dbText.textContent = `Non connesso · ${out?.reason || "n/d"}`;
    return;
  }
  if (!out.latest_run) {
    el.dbText.textContent = "Connesso · nessun aggiornamento registrato";
    return;
  }
  const latest = out.latest_run;
  const context = runContextSummary(latest, { includeMethods: false, fallbackDefault: false });
  const status = latest.status_label || latest.status || "";
  const parts = ["Connesso", `ultimo ${runTypeLabel(latest)}`];
  if (context) parts.push(context);
  if (status) parts.push(status);
  parts.push(`#${shortRunCode(latest.run_id)}`);
  el.dbText.textContent = parts.join(" · ");
}

function buildRunsQuery() {
  const params = new URLSearchParams();
  params.set("limit", String(state.runsLimit));
  params.set("offset", String(state.runsOffset));

  const q = (el.runsSearch?.value || "").trim();
  const source = (el.runsSourceFilter?.value || "all").trim();
  const status = (el.runsStatusFilter?.value || "").trim();
  const sortBy = (el.runsSortBy?.value || "started_at").trim();
  const sortDir = (el.runsSortDir?.value || "desc").trim();
  const runType = (el.runsTypeFilter?.value || "").trim();

  if (q) params.set("q", q);
  if (source && source !== "all") params.set("source", source);
  if (status) params.set("status", status);
  if (sortBy) params.set("sort_by", sortBy);
  if (sortDir) params.set("sort_dir", sortDir);
  if (runType) params.set("run_type", runType);
  return params.toString();
}

function handleRunsFilterChanged() {
  state.runsOffset = 0;
  const limitVal = Number(el.runsPageSize?.value || state.runsLimit || 40);
  state.runsLimit = Number.isFinite(limitVal) && limitVal > 0 ? limitVal : 40;
  if (runsFilterDebounce) clearTimeout(runsFilterDebounce);
  runsFilterDebounce = setTimeout(() => {
    refreshRuns();
    refreshSelectedRunDetails();
  }, 220);
}

function renderRunsPager() {
  const page = Math.floor(state.runsOffset / state.runsLimit) + 1;
  const totalPages = Math.max(1, Math.ceil((state.runsTotal || 0) / state.runsLimit));
  if (el.runsPageInfo) {
    el.runsPageInfo.textContent = `Pagina ${page}/${totalPages} · ${state.runsTotal} run`;
  }
  if (el.runsPrevPageBtn) {
    el.runsPrevPageBtn.disabled = state.runsOffset <= 0;
  }
  if (el.runsNextPageBtn) {
    el.runsNextPageBtn.disabled = state.runsOffset + state.runsLimit >= state.runsTotal;
  }
}

function setDashboardWarn(message) {
  if (!el.dashboardWarn) return;
  if (!message) {
    el.dashboardWarn.classList.add("hidden");
    el.dashboardWarn.textContent = "";
    return;
  }
  el.dashboardWarn.classList.remove("hidden");
  el.dashboardWarn.textContent = message;
}

function kpiTrendClass(key, absDelta) {
  const higherBetter = new Set(["avg_sellout_clamped"]);
  const lowerBetter = new Set(["critical_rows_total", "critical_deficit_total", "next_current_positive_delta_count", "next_current_delta_positive_total"]);
  const d = Number(absDelta);
  if (!Number.isFinite(d) || Math.abs(d) < 1e-9) return "neutral";
  if (higherBetter.has(key)) return d > 0 ? "good" : "alert";
  if (lowerBetter.has(key)) return d < 0 ? "good" : "alert";
  return "neutral";
}

function formatKpiDelta(deltaObj, mode = "abs", digits = 2, unit = "count") {
  if (!deltaObj) return "baseline non disponibile";
  const absV = Number(deltaObj.abs);
  const pctV = deltaObj.pct == null ? null : Number(deltaObj.pct);
  if (!Number.isFinite(absV)) return "baseline non disponibile";
  if (Math.abs(absV) < 1e-9) return "allineato al prec. aggiornamento";

  if (mode === "pct") {
    if (pctV == null || !Number.isFinite(pctV)) return "baseline non disponibile";
    const sign = pctV > 0 ? "+" : "";
    return `${sign}${fmtNum(pctV, 1)}% vs agg. precedente`;
  }

  if (mode === "pp") {
    const sign = absV > 0 ? "+" : "";
    return `${sign}${fmtNum(absV, 2)} pt vs agg. precedente`;
  }

  const sign = absV > 0 ? "+" : "-";
  return `${sign}${formatMetricValue(unit, Math.abs(absV), digits)} vs agg. precedente`;
}

function selloutPct(value) {
  const v = Number(value || 0);
  if (!Number.isFinite(v)) return 0;
  return v <= 1 ? v * 100 : v;
}

const UNIT_META = {
  count: {
    label: "Conteggio",
    description: "Numero di negozi, articoli o righe presenti nell'aggiornamento.",
  },
  pairs: {
    label: "Paia",
    description: "Quantità fisiche suggerite, trasferite, mancanti o ordinate.",
  },
  euro: {
    label: "Euro",
    description: "Valore economico stimato dei suggerimenti ordine.",
  },
  percent: {
    label: "Percentuale",
    description: "Indicatore percentuale, ad esempio il sellout medio.",
  },
  factor: {
    label: "Fattore",
    description: "Moltiplicatore usato nella stima della prossima stagione.",
  },
};

const KPI_META = {
  avg_sellout_clamped: {
    label: "Sellout medio",
    unit: "percent",
    digits: 2,
    deltaMode: "pp",
    deltaDigits: 2,
    description: "Rotazione media della merce sul consegnato.",
    format: (kpis) => (kpis.avg_sellout_clamped == null ? "--" : fmtPercentValue(selloutPct(kpis.avg_sellout_clamped), 2)),
  },
  critical_deficit_total: {
    label: "Mancanza stimata totale",
    unit: "pairs",
    digits: 2,
    deltaMode: "abs",
    deltaDigits: 2,
    description: "Paia mancanti dove la domanda stimata supera lo stock dopo i movimenti.",
    format: (kpis) => fmtPairs(kpis.critical_deficit_total, 2),
  },
  transfer_qty_total: {
    label: "Paia da trasferire",
    unit: "pairs",
    digits: 2,
    deltaMode: "abs",
    deltaDigits: 2,
    description: "Volume totale dei trasferimenti suggeriti.",
    format: (kpis) => fmtPairs(kpis.transfer_qty_total, 2),
  },
  order_budget_total: {
    label: "Valore ordini",
    unit: "euro",
    digits: 2,
    deltaMode: "abs",
    deltaDigits: 2,
    description: "Budget economico stimato dell'intero forecast ordini.",
    format: (kpis) => fmtCurrency(kpis.order_budget_total, 2),
  },
  transfer_rows: {
    label: "Righe trasferimento",
    unit: "count",
    digits: 0,
    deltaMode: "abs",
    deltaDigits: 0,
    description: "Numero di proposte aggregate di trasferimento.",
    format: (kpis) => fmtNum(kpis.transfer_rows, 0),
  },
  order_qty_total: {
    label: "Paia ordinate",
    unit: "pairs",
    digits: 2,
    deltaMode: "abs",
    deltaDigits: 2,
    description: "Quantità totale suggerita nel modulo ordini.",
    format: (kpis) => fmtPairs(kpis.order_qty_total, 2),
  },
  critical_rows_total: {
    label: "Righe critiche",
    unit: "count",
    digits: 0,
    deltaMode: "abs",
    deltaDigits: 0,
    description: "Combinazioni negozio-articolo in cui la domanda stimata supera lo stock.",
    format: (kpis) => fmtNum(kpis.critical_rows_total, 0),
  },
  next_current_budget_total: {
    label: "Valore prossima stagione",
    unit: "euro",
    digits: 2,
    deltaMode: "abs",
    deltaDigits: 2,
    description: "Valore acquisto stimato dei candidati continuativi per la stagione successiva.",
    format: (kpis) => fmtCurrency(kpis.next_current_budget_total, 2),
  },
  next_current_delta_positive_total: {
    label: "Extra stimato totale",
    unit: "pairs",
    digits: 2,
    deltaMode: "abs",
    deltaDigits: 2,
    description: "Paia aggiuntive da coprire per i candidati con fabbisogno positivo.",
    format: (kpis) => fmtPairs(kpis.next_current_delta_positive_total, 2),
  },
  next_current_positive_delta_count: {
    label: "Candidati con extra",
    unit: "count",
    digits: 0,
    deltaMode: "abs",
    deltaDigits: 0,
    description: "Articoli continuativi che richiedono più stock per la prossima stagione.",
    format: (kpis) => fmtNum(kpis.next_current_positive_delta_count, 0),
  },
  transfer_avg_qty: {
    label: "Media paia per trasferimento",
    unit: "pairs",
    digits: 2,
    deltaMode: "abs",
    deltaDigits: 2,
    description: "Dimensione media di ogni proposta di trasferimento.",
    format: (kpis) => fmtPairs(kpis.transfer_avg_qty, 2),
  },
  source_shops: {
    label: "Negozi origine",
    unit: "count",
    digits: 0,
    deltaMode: "abs",
    deltaDigits: 0,
    description: "Numero di negozi che cedono merce.",
    format: (kpis) => fmtNum(kpis.source_shops, 0),
  },
  target_shops: {
    label: "Negozi destinazione",
    unit: "count",
    digits: 0,
    deltaMode: "abs",
    deltaDigits: 0,
    description: "Numero di negozi che ricevono merce.",
    format: (kpis) => fmtNum(kpis.target_shops, 0),
  },
  order_rows: {
    label: "Righe forecast ordini",
    unit: "count",
    digits: 0,
    deltaMode: "abs",
    deltaDigits: 0,
    description: "Numero di righe prodotte nel forecast ordini.",
    format: (kpis) => fmtNum(kpis.order_rows, 0),
  },
  feature_rows: {
    label: "Righe negozio-articolo",
    unit: "count",
    digits: 0,
    deltaMode: "abs",
    deltaDigits: 0,
    description: "Copertura totale delle analisi articolo per negozio.",
    format: (kpis) => fmtNum(kpis.feature_rows, 0),
  },
  shop_count: {
    label: "Negozi",
    unit: "count",
    digits: 0,
    deltaMode: "abs",
    deltaDigits: 0,
    description: "Numero di negozi presenti nell'aggiornamento selezionato.",
    format: (kpis) => fmtNum(kpis.shop_count, 0),
  },
  article_count: {
    label: "Articoli",
    unit: "count",
    digits: 0,
    deltaMode: "abs",
    deltaDigits: 0,
    description: "Numero di articoli unici presenti nella run.",
    format: (kpis) => fmtNum(kpis.article_count, 0),
  },
  sales_rows: {
    label: "Righe vendita importate",
    unit: "count",
    digits: 0,
    deltaMode: "abs",
    deltaDigits: 0,
    description: "Righe dello snapshot vendite caricate per la run.",
    format: (kpis) => fmtNum(kpis.sales_rows, 0),
  },
  stock_rows: {
    label: "Righe stock importate",
    unit: "count",
    digits: 0,
    deltaMode: "abs",
    deltaDigits: 0,
    description: "Righe dello snapshot stock caricate per la run.",
    format: (kpis) => fmtNum(kpis.stock_rows, 0),
  },
  next_current_candidates: {
    label: "Candidati prossima stagione",
    unit: "count",
    digits: 0,
    deltaMode: "abs",
    deltaDigits: 0,
    description: "Articoli continuativi osservati come possibili candidati per la stagione successiva.",
    format: (kpis) => fmtNum(kpis.next_current_candidates, 0),
  },
  next_current_qty_total: {
    label: "Paia prossima stagione",
    unit: "pairs",
    digits: 2,
    deltaMode: "abs",
    deltaDigits: 2,
    description: "Quantità totale stimata per i candidati della prossima stagione.",
    format: (kpis) => fmtPairs(kpis.next_current_qty_total, 2),
  },
};

const KPI_ORDER = [
  "avg_sellout_clamped",
  "critical_deficit_total",
  "transfer_qty_total",
  "order_budget_total",
  "transfer_rows",
  "order_qty_total",
  "critical_rows_total",
  "next_current_budget_total",
  "next_current_delta_positive_total",
  "next_current_positive_delta_count",
  "transfer_avg_qty",
  "source_shops",
  "target_shops",
  "order_rows",
  "feature_rows",
  "shop_count",
  "article_count",
  "sales_rows",
  "stock_rows",
  "next_current_candidates",
  "next_current_qty_total",
];

const HERO_KPI_KEYS = [
  "transfer_qty_total",
  "order_budget_total",
  "avg_sellout_clamped",
  "critical_deficit_total",
];

function renderDashboardLegend() {
  if (!el.dashboardLegend) return;
  const items = ["count", "pairs", "euro", "percent", "factor"];
  el.dashboardLegend.innerHTML = items
    .map((unit) => {
      const meta = UNIT_META[unit];
      return `
        <article class="legend-item">
          <span class="legend-badge unit-${escHtml(unit)}">${escHtml(meta.label)}</span>
          <span class="legend-text">${escHtml(meta.description)}</span>
        </article>
      `;
    })
    .join("");
}

function renderDashboardQuickFacts(kpis) {
  if (!el.dashboardQuickFacts) return;
  el.dashboardQuickFacts.innerHTML = HERO_KPI_KEYS
    .map((key) => {
      const meta = KPI_META[key];
      return `
        <article class="quick-fact-card">
          <div class="quick-fact-head">
            <span class="quick-fact-label">${escHtml(meta.label)}</span>
            <span class="kpi-unit unit-${escHtml(meta.unit)}">${escHtml(unitBadgeLabel(meta.unit))}</span>
          </div>
          <div class="quick-fact-value">${escHtml(meta.format(kpis || {}))}</div>
          <div class="quick-fact-text">${escHtml(meta.description)}</div>
        </article>
      `;
    })
    .join("");
}

function renderDashboardKpis(kpis, kpiDeltas = {}) {
  if (!el.dashboardKpis) return;
  const cards = KPI_ORDER.map((key) => {
    const meta = KPI_META[key];
    return {
      key,
      label: meta.label,
      unit: meta.unit,
      description: meta.description,
      value: meta.format(kpis || {}),
      deltaMode: meta.deltaMode || "abs",
      deltaDigits: meta.deltaDigits ?? meta.digits ?? 2,
    };
  });

  cards.push({
    key: "season_qty_delta_pct",
    label: "Trend coppia stagionale",
    unit: "percent",
    description: "Variazione del volume della coppia stagionale più recente rispetto alla stessa coppia dell'anno precedente.",
    value:
      kpis.season_qty_delta_pct == null
        ? "--"
        : `${Number(kpis.season_qty_delta_pct) > 0 ? "+" : ""}${fmtPercentValue(kpis.season_qty_delta_pct, 1)}`,
    seasonalText:
      kpis.season_latest_code && kpis.season_prev_code
        ? `${kpis.season_latest_code} vs ${kpis.season_prev_code}`
        : "storia stagionale insufficiente",
  });

  el.dashboardKpis.innerHTML = cards
    .map((c) => {
      const d = kpiDeltas?.[c.key];
      const trend =
        c.key === "season_qty_delta_pct"
          ? (kpis.season_qty_delta_pct == null ? "neutral" : (Number(kpis.season_qty_delta_pct) >= 0 ? "good" : "alert"))
          : kpiTrendClass(c.key, d?.abs);
      const deltaText = c.seasonalText || formatKpiDelta(d, c.deltaMode || "abs", c.deltaDigits ?? 2, c.unit || "count");
      return `
        <article class="kpi-card trend-${escHtml(trend)}">
          <div class="kpi-topline">
            <div class="kpi-label">${escHtml(c.label)}</div>
            <span class="kpi-unit unit-${escHtml(c.unit || "count")}" title="${escHtml((UNIT_META[c.unit || "count"] || {}).description || "")}">
              ${escHtml(unitBadgeLabel(c.unit || "count"))}
            </span>
          </div>
          <div class="kpi-value">${escHtml(c.value)}</div>
          <div class="kpi-help">${escHtml(c.description || "")}</div>
          <div class="kpi-delta trend-${escHtml(trend)}">${escHtml(deltaText)}</div>
        </article>
      `;
    })
    .join("");
}

function renderInsights(kpis) {
  if (!el.insightBoard) return;
  if (!kpis || Object.keys(kpis).length === 0) {
    el.insightBoard.innerHTML = `
      <article class="insight-card info">
        <p class="insight-title">Seleziona un aggiornamento</p>
        <p class="insight-text">Le letture qualitative compaiono quando la dashboard carica una run dal database.</p>
      </article>
    `;
    return;
  }
  const avgSellout = selloutPct(kpis.avg_sellout_clamped);
  const deficitTot = Number(kpis.critical_deficit_total || 0);
  const deficitRows = Number(kpis.critical_rows_total || 0);
  const transferAvg = Number(kpis.transfer_avg_qty || 0);
  const nextDeltaPos = Number(kpis.next_current_delta_positive_total || 0);
  const nextDeltaCount = Number(kpis.next_current_positive_delta_count || 0);

  const insights = [];

  if (avgSellout >= 62) {
    insights.push({
      level: "good",
      title: "Sellout robusto",
      text: `Sellout medio ${fmtPercentValue(avgSellout, 2)}: la rotazione è solida.`,
    });
  } else if (avgSellout >= 42) {
    insights.push({
      level: "warn",
      title: "Sellout da migliorare",
      text: `Sellout medio ${fmtPercentValue(avgSellout, 2)}: verifica mix articoli per negozio.`,
    });
  } else {
    insights.push({
      level: "alert",
      title: "Sellout debole",
      text: `Sellout medio ${fmtPercentValue(avgSellout, 2)}: rischio over-stock elevato.`,
    });
  }

  if (deficitRows === 0 || deficitTot <= 0) {
    insights.push({
      level: "good",
      title: "Deficit sotto controllo",
      text: "Nessun deficit critico rilevato nell'aggiornamento selezionato.",
    });
  } else if (deficitTot < 500) {
    insights.push({
      level: "warn",
      title: "Deficit moderato",
      text: `${fmtNum(deficitRows, 0)} righe critiche, mancanza stimata ${fmtPairs(deficitTot, 2)}.`,
    });
  } else {
    insights.push({
      level: "alert",
      title: "Deficit alto",
      text: `${fmtNum(deficitRows, 0)} righe critiche, mancanza stimata ${fmtPairs(deficitTot, 2)}: priorità a riallocazioni.`,
    });
  }

  if (transferAvg >= 4.0) {
    insights.push({
      level: "warn",
      title: "Movimenti pesanti",
      text: `Media per trasferimento ${fmtPairs(transferAvg, 2)}: verifica impatto operativo.`,
    });
  } else {
    insights.push({
      level: "info",
      title: "Movimenti bilanciati",
      text: `Media per trasferimento ${fmtPairs(transferAvg, 2)}.`,
    });
  }

  if (nextDeltaCount > 0 && nextDeltaPos > 0) {
    insights.push({
      level: "info",
      title: "Opportunità Next Season",
      text: `${fmtNum(nextDeltaCount, 0)} candidati con extra positivo per ${fmtPairs(nextDeltaPos, 2)} complessive.`,
    });
  } else {
    insights.push({
      level: "good",
      title: "Next Season stabile",
      text: "Nessun candidato con delta positivo marcato.",
    });
  }

  const priority = { alert: 0, warn: 1, info: 2, good: 3 };
  insights.sort((a, b) => (priority[a.level] ?? 9) - (priority[b.level] ?? 9));

  el.insightBoard.innerHTML = insights
    .map(
      (it) => `
        <article class="insight-card ${escHtml(it.level)}">
          <p class="insight-title">${escHtml(it.title)}</p>
          <p class="insight-text">${escHtml(it.text)}</p>
        </article>
      `,
    )
    .join("");
}

function renderBarChart(container, rows, options = {}) {
  if (!container) return;
  const cfg = typeof options === "number" ? { digits: options } : (options || {});
  const unit = cfg.unit || "count";
  const digits = cfg.digits ?? 2;
  if (!rows || rows.length === 0) {
    container.innerHTML = "<div class='empty-state'>Nessun dato disponibile per questo aggiornamento.</div>";
    return;
  }
  const maxVal = Math.max(...rows.map((r) => Number(r.value) || 0), 0.0001);
  container.innerHTML = rows
    .map((r) => {
      const value = Number(r.value) || 0;
      const width = Math.max(2, Math.round((value / maxVal) * 100));
      return `
        <div class="bar-row">
          <div class="bar-label" title="${escHtml(r.label)}">${escHtml(r.label)}</div>
          <div class="bar-track"><div class="bar-fill" style="width:${width}%"></div></div>
          <div class="bar-value">${escHtml(formatMetricValue(unit, value, digits))}</div>
        </div>
      `;
    })
    .join("");
}

function metricCellClass(tableKey, key, raw) {
  const n = Number(raw);
  if (!Number.isFinite(n)) return "";
  if (tableKey === "critical_articles" && key === "deficit") {
    if (n >= 30) return "metric-alert";
    if (n >= 10) return "metric-warn";
    if (n > 0) return "metric-good";
  }
  if (tableKey === "next_current_candidates" && key === "delta_vs_stock") {
    if (n >= 20) return "metric-alert";
    if (n >= 5) return "metric-warn";
    if (n > 0) return "metric-good";
  }
  if (tableKey === "order_proposals" && key === "budget_acquisto") {
    if (n >= 10000) return "metric-alert";
    if (n >= 3000) return "metric-warn";
  }
  return "";
}

function formatDashboardCellValue(tableKey, key, raw, row) {
  if (raw == null || raw === "") return "--";

  if (tableKey === "transfer_proposals") {
    if (key === "qty") return fmtPairs(raw, 2);
    if (key === "size") return fmtNum(raw, 0);
    return fmt(raw);
  }

  if (tableKey === "order_proposals") {
    if (key === "module") return friendlyModuleLabel(raw);
    if (key === "season_code") return friendlySeasonLabel(raw, row?.module || null);
    if (key === "mode") return friendlyModeLabel(raw);
    if (key === "prezzo_listino" || key === "prezzo_vendita") return fmtCurrency(raw, 2);
    if (key === "totale_qty" || key === "predizione_vendite") return fmtPairs(raw, 2);
    if (key === "budget_acquisto") return fmtCurrency(raw, 2);
    return fmt(raw);
  }

  if (tableKey === "critical_articles") {
    if (key === "demand_hybrid" || key === "stock_after" || key === "deficit") return fmtPairs(raw, 2);
    return fmt(raw);
  }

  if (tableKey === "next_current_candidates") {
    if (key === "from_cont_season") return friendlySeasonLabel(raw, "continuativa");
    if (key === "prezzo_vendita") return fmtCurrency(raw, 2);
    if (key === "venduto_periodo" || key === "giacenza" || key === "predicted_current_qty" || key === "delta_vs_stock") return fmtPairs(raw, 2);
    if (key === "predicted_budget") return fmtCurrency(raw, 2);
    if (key === "applied_factor") return fmtFactor(raw, 2);
    if (key === "transition_score") return fmtNum(raw, 4);
    return fmt(raw);
  }

  return fmt(raw);
}

function renderDashboardTable(tbody, rows, keys, numericKeys = [], tableKey = "") {
  if (!tbody) return;
  if (!rows || rows.length === 0) {
    tbody.innerHTML = `<tr class='empty-row'><td colspan='${keys.length}'>Nessun dato disponibile</td></tr>`;
    return;
  }
  tbody.innerHTML = rows
    .map((row) => {
      const cells = keys
        .map((k) => {
          const raw = row[k];
          const val = formatDashboardCellValue(tableKey, k, raw, row) || (numericKeys.includes(k) ? fmtNum(raw, 2) : fmt(raw));
          const cls = metricCellClass(tableKey, k, raw);
          return `<td class="${escHtml(cls)}">${escHtml(val)}</td>`;
        })
        .join("");
      return `<tr>${cells}</tr>`;
    })
    .join("");
}

function normalizeText(v) {
  return String(v == null ? "" : v).toLowerCase();
}

function compareDashboardValues(a, b, numeric = false) {
  if (numeric) {
    const na = Number(a);
    const nb = Number(b);
    const va = Number.isFinite(na) ? na : Number.NEGATIVE_INFINITY;
    const vb = Number.isFinite(nb) ? nb : Number.NEGATIVE_INFINITY;
    if (va < vb) return -1;
    if (va > vb) return 1;
    return 0;
  }
  const sa = normalizeText(a);
  const sb = normalizeText(b);
  return sa.localeCompare(sb, "it", { numeric: true, sensitivity: "base" });
}

function getDashboardTableRows(tableKey) {
  const cfg = DASHBOARD_TABLE_CONFIG[tableKey];
  if (!cfg) return { total: 0, filtered: 0, shown: 0, rows: [], exportRows: [] };
  const raw = Array.isArray(state.dashboardData?.tables?.[tableKey]) ? state.dashboardData.tables[tableKey] : [];
  const tState = state.dashboardTableState[tableKey] || {
    sortKey: cfg.columns[0],
    sortDir: "asc",
    search: "",
    rowLimit: 20,
    showAll: false,
  };
  const searchNorm = normalizeText(tState.search || "").trim();
  let filtered = raw;
  if (searchNorm) {
    filtered = raw.filter((row) => cfg.columns.some((k) => normalizeText(row[k]).includes(searchNorm)));
  }
  const sorted = [...filtered];
  const sortKey = tState.sortKey;
  const numeric = cfg.numericColumns.includes(sortKey);
  sorted.sort((ra, rb) => {
    const c = compareDashboardValues(ra[sortKey], rb[sortKey], numeric);
    return tState.sortDir === "desc" ? -c : c;
  });
  const limit = Number.isFinite(Number(tState.rowLimit)) ? Number(tState.rowLimit) : 20;
  const shownRows = tState.showAll ? sorted : sorted.slice(0, Math.max(1, limit));
  return {
    total: raw.length,
    filtered: sorted.length,
    shown: shownRows.length,
    rows: shownRows,
    exportRows: sorted,
  };
}

function setDashboardTableSortIndicators(tableKey) {
  const cfg = DASHBOARD_TABLE_CONFIG[tableKey];
  if (!cfg || !cfg.tableEl) return;
  const tState = state.dashboardTableState[tableKey];
  cfg.tableEl.querySelectorAll("th.sortable").forEach((th) => {
    th.classList.remove("sorted-asc", "sorted-desc");
    if (th.dataset.key === tState.sortKey) {
      th.classList.add(tState.sortDir === "desc" ? "sorted-desc" : "sorted-asc");
    }
  });
}

function renderDashboardTableByKey(tableKey) {
  const cfg = DASHBOARD_TABLE_CONFIG[tableKey];
  if (!cfg) return;
  const data = getDashboardTableRows(tableKey);
  renderDashboardTable(cfg.tbodyEl, data.rows, cfg.columns, cfg.numericColumns, tableKey);
  setDashboardTableSortIndicators(tableKey);
  if (cfg.infoEl) {
    cfg.infoEl.textContent = `${data.shown}/${data.filtered} (tot ${data.total})`;
  }
  if (cfg.showAllEl) {
    const isAll = !!state.dashboardTableState[tableKey]?.showAll;
    cfg.showAllEl.textContent = isAll ? "Mostra Top" : "Mostra Tutte";
  }
  if (cfg.focusEl) {
    const isFull = state.fullscreenTableKey === tableKey;
    cfg.focusEl.textContent = isFull ? "Chiudi Schermo Intero" : "Schermo Intero";
  }
}

function renderAllDashboardTables() {
  Object.keys(DASHBOARD_TABLE_CONFIG).forEach((tableKey) => renderDashboardTableByKey(tableKey));
}

function escapeCsvCell(v) {
  const s = String(v == null ? "" : v);
  if (s.includes('"') || s.includes(",") || s.includes("\n") || s.includes("\r")) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function buildCsvContent(rows, cols) {
  const lines = [];
  lines.push(cols.map((c) => escapeCsvCell(c)).join(","));
  rows.forEach((row) => {
    lines.push(cols.map((c) => escapeCsvCell(row[c])).join(","));
  });
  return lines.join("\r\n");
}

function downloadBlob(blob, filename) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function exportDashboardTableCsv(tableKey) {
  const cfg = DASHBOARD_TABLE_CONFIG[tableKey];
  if (!cfg) return;
  const data = getDashboardTableRows(tableKey);
  const csv = buildCsvContent(data.exportRows, cfg.columns);
  const runShort = state.dashboardRunId ? String(state.dashboardRunId).slice(0, 8) : "na";
  const ts = new Date().toISOString().replace(/[-:]/g, "").replace(/\..+/, "");
  const filename = `barca_${tableKey}_${runShort}_${ts}.csv`;
  downloadBlob(new Blob([csv], { type: "text/csv;charset=utf-8;" }), filename);
}

async function exportDashboardTableXlsx(tableKey) {
  if (!state.dashboardRunId) {
    alert("Seleziona prima un aggiornamento dashboard.");
    return;
  }
  const qs = new URLSearchParams({
    table_key: tableKey,
    run_id: state.dashboardRunId,
    fmt: "xlsx",
    table_limit: "50000",
  });
  const res = await fetch(`/api/dashboard/export?${qs.toString()}`);
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(txt || `HTTP ${res.status}`);
  }
  const blob = await res.blob();
  let filename = `barca_${tableKey}.xlsx`;
  const cd = res.headers.get("content-disposition") || "";
  const m = /filename="([^"]+)"/i.exec(cd);
  if (m && m[1]) filename = m[1];
  downloadBlob(blob, filename);
}

function toggleTableFullscreen(tableKey, forceState = null) {
  const cfg = DASHBOARD_TABLE_CONFIG[tableKey];
  if (!cfg || !cfg.panelEl) return;

  const shouldOpen =
    forceState == null
      ? state.fullscreenTableKey !== tableKey
      : Boolean(forceState);

  Object.keys(DASHBOARD_TABLE_CONFIG).forEach((k) => {
    const panel = DASHBOARD_TABLE_CONFIG[k].panelEl;
    if (panel) panel.classList.remove("fullscreen");
  });

  if (shouldOpen) {
    cfg.panelEl.classList.add("fullscreen");
    state.fullscreenTableKey = tableKey;
    document.body.classList.add("lock-scroll");
  } else {
    state.fullscreenTableKey = null;
    document.body.classList.remove("lock-scroll");
  }
  renderAllDashboardTables();
}

function renderRunContextPills(run, baselineRun = null) {
  if (!el.dashboardRunContext) return;
  if (!run) {
    el.dashboardRunContext.innerHTML = "";
    return;
  }
  const pills = [];
  pills.push(runTypeLabel(run));
  const ctx = run.business_context || {};
  const currentSeasons = seasonLabelsForRun(ctx, "current", "current_seasons");
  const contSeasons = seasonLabelsForRun(ctx, "continuativa", "continuativa_seasons");
  const currentModes = modeLabelsForRun(ctx, "current", "current_modes");
  const contModes = modeLabelsForRun(ctx, "continuativa", "continuativa_modes");
  if (currentSeasons.length > 0) {
    pills.push(`Stagione corrente: ${currentSeasons.join(", ")}`);
  }
  if (contSeasons.length > 0) {
    pills.push(`Continuativa: ${contSeasons.join(", ")}`);
  }
  if (currentModes.length > 0) {
    pills.push(`Metodo corrente: ${currentModes.join(", ")}`);
  }
  if (contModes.length > 0) {
    pills.push(`Metodo continuativa: ${contModes.join(", ")}`);
  }
  if (baselineRun?.run_id) {
    const baselineContext = runContextSummary(baselineRun, { includeMethods: false, fallbackDefault: false });
    pills.push(`Confronto con ${baselineContext || `#${shortRunCode(baselineRun.run_id)}`}`);
  }
  if (Array.isArray(ctx.notes) && ctx.notes.length > 0) {
    ctx.notes.forEach((note) => pills.push(note));
  }
  el.dashboardRunContext.innerHTML = pills
    .map((txt) => `<span class="context-pill">${escHtml(txt)}</span>`)
    .join("");
}

function dashboardRunLabel(run) {
  if (!run || !run.run_id) return "aggiornamento non valido";
  const started = fmtDateCompact(run.started_at);
  const typeLabel = runTypeLabel(run);
  const context = runContextSummary(run, { includeMethods: false, fallbackDefault: false });
  const parts = [started];
  if (context) parts.push(context);
  parts.push(typeLabel, `#${shortRunCode(run.run_id)}`);
  return parts.join(" · ");
}

async function api(url, options = {}) {
  const res = await fetch(url, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const txt = await res.text();
  let payload = {};
  try {
    payload = txt ? JSON.parse(txt) : {};
  } catch {
    payload = { raw: txt };
  }
  if (!res.ok) {
    const msg = payload.detail || payload.raw || `HTTP ${res.status}`;
    throw new Error(msg);
  }
  return payload;
}

async function loadSettings() {
  const settings = await api("/api/settings");
  state.developerMode = !!settings.developer_mode;
  if (el.catalogPhotoRoot) {
    el.catalogPhotoRoot.value = settings.catalog_photo_root || "";
  }
  if (el.catalogSettingsMsg) {
    el.catalogSettingsMsg.textContent = settings.catalog_photo_root
      ? `Percorso attuale: ${settings.catalog_photo_root}`
      : "Percorso foto non impostato.";
  }
  renderDeveloperMode();
}

async function saveCatalogSettings() {
  if (!el.catalogSettingsMsg) return;
  el.catalogSettingsMsg.textContent = "Salvataggio impostazioni catalogo...";
  try {
    const out = await api("/api/settings/catalog", {
      method: "POST",
      body: JSON.stringify({
        catalog_photo_root: el.catalogPhotoRoot?.value?.trim() || "",
      }),
    });
    if (el.catalogPhotoRoot) {
      el.catalogPhotoRoot.value = out.catalog_photo_root || "";
    }
    el.catalogSettingsMsg.textContent = out.catalog_photo_root
      ? `Percorso foto salvato: ${out.catalog_photo_root}`
      : "Percorso foto catalogo rimosso.";
  } catch (err) {
    el.catalogSettingsMsg.textContent = `Errore impostazioni catalogo: ${err.message}`;
  }
}

function renderDeveloperMode() {
  el.devModeBtn.textContent = `Modalita sviluppatore: ${state.developerMode ? "ON" : "OFF"}`;
  if (state.developerMode) {
    el.devModeBtn.classList.add("warn");
    el.developerPanel.classList.remove("hidden");
  } else {
    el.devModeBtn.classList.remove("warn");
    el.developerPanel.classList.add("hidden");
    el.logsBox.textContent = "Modalita sviluppatore OFF";
    if (state.activeView === "dev") {
      setActiveView("dashboard");
    }
  }
  if (el.tabDev) {
    el.tabDev.disabled = !state.developerMode;
  }
}

function setActiveView(viewName) {
  const target = String(viewName || "dashboard").toLowerCase();
  if (target === "dev" && !state.developerMode) {
    alert("Attiva prima la modalita sviluppatore per aprire la sezione Sviluppo.");
    return;
  }
  state.activeView = target;
  el.viewTabs.forEach((btn) => {
    const isActive = btn.dataset.viewTarget === target;
    btn.classList.toggle("active", isActive);
  });
  el.viewPanels.forEach((panel) => {
    const show = panel.dataset.view === target;
    panel.classList.toggle("section-hidden", !show);
  });
  if (target !== "dashboard" && state.fullscreenTableKey) {
    toggleTableFullscreen(state.fullscreenTableKey, false);
  }
}

function setActiveDashSection(sectionName) {
  const target = String(sectionName || "transfers").toLowerCase();
  if (target !== state.activeDashSection && state.fullscreenTableKey) {
    toggleTableFullscreen(state.fullscreenTableKey, false);
  }
  state.activeDashSection = target;
  el.dashSectionTabs.forEach((btn) => {
    const isActive = btn.dataset.dashTarget === target;
    btn.classList.toggle("active", isActive);
  });
  el.dashSections.forEach((section) => {
    const show = section.dataset.dashSection === target;
    section.classList.toggle("active", show);
  });
}

function collectRunPayload() {
  const val = (id) => document.getElementById(id).value.trim();
  const chk = (id) => document.getElementById(id).checked;
  return {
    source_db: chk("sourceDb"),
    source_db_run_id: val("sourceDbRunId") || null,
    skip_ingest: chk("skipIngest"),
    incoming_root: val("incomingRoot") || null,
    keep_incoming: false,
    skip_orders: chk("skipOrders"),
    orders_root: val("ordersRoot") || null,
    orders_source_db: chk("ordersSourceDb"),
    orders_source_db_run_id: val("ordersSourceDbRunId") || null,
    orders_math_only: chk("ordersMathOnly"),
    orders_coverage: Number(document.getElementById("ordersCoverage").value || "1.20"),
    sync_db: chk("syncDb"),
    db_create_schema: chk("createSchema"),
  };
}

async function startRun(evt) {
  evt.preventDefault();
  el.runFormMsg.textContent = "Avvio aggiornamento...";
  try {
    const payload = collectRunPayload();
    const out = await api("/api/run", { method: "POST", body: JSON.stringify(payload) });
    state.selectedRunId = out.run.run_id;
    state.runsOffset = 0;
    el.runFormMsg.textContent = `Aggiornamento avviato: #${shortRunCode(out.run.run_id)}`;
    await refreshRuns();
  } catch (err) {
    el.runFormMsg.textContent = `Errore: ${err.message}`;
  }
}

async function toggleDeveloperMode() {
  try {
    const out = await api("/api/settings/developer-mode", {
      method: "POST",
      body: JSON.stringify({ enabled: !state.developerMode }),
    });
    state.developerMode = !!out.developer_mode;
    renderDeveloperMode();
    await refreshSelectedRunDetails();
  } catch (err) {
    alert(`Errore modalita sviluppatore: ${err.message}`);
  }
}

async function refreshHealth() {
  try {
    const out = await api("/api/health");
    state.activeRunId = out.active_run_id || null;
    el.healthText.textContent = `${out.ok ? "ONLINE" : "OFFLINE"} - ${fmt(out.time)}`;
    renderActiveRunText();
  } catch (err) {
    el.healthText.textContent = `Errore: ${err.message}`;
    state.activeRunId = null;
    el.activeRunText.textContent = "--";
  }
}

async function refreshDb() {
  try {
    const out = await api("/api/db/status");
    renderDbSummary(out);
    el.dbStatusBox.textContent = JSON.stringify(out, null, 2);
  } catch (err) {
    el.dbText.textContent = `Errore DB: ${err.message}`;
    el.dbStatusBox.textContent = `Errore DB: ${err.message}`;
  }
}

async function refreshOutputs() {
  try {
    const out = await api("/api/outputs");
    const html = out.files
      .map((f) => {
        const exists = f.exists ? "SI" : "NO";
        return `
          <tr>
            <td>${f.file}</td>
            <td>${exists}</td>
            <td>${fmt(f.rows)}</td>
            <td>${fmtBytes(f.size_bytes)}</td>
            <td>${fmt(f.modified_at)}</td>
          </tr>`;
      })
      .join("");
    el.outputsTableBody.innerHTML = html || "<tr><td colspan='5'>Nessun file</td></tr>";
  } catch (err) {
    el.outputsTableBody.innerHTML = `<tr><td colspan='5'>Errore: ${err.message}</td></tr>`;
  }
}

function renderCatalogSummary(status) {
  if (!el.catalogSummary) return;
  if (!status?.available) {
    el.catalogSummary.innerHTML = `
      <article class="catalog-summary-card">
        <span class="catalog-summary-label">Catalogo</span>
        <strong class="catalog-summary-value">Non disponibile</strong>
        <span class="catalog-summary-note">${escHtml(status?.reason || "Nessuna importazione catalogo disponibile.")}</span>
      </article>
    `;
    return;
  }

  const run = status.run || {};
  const counts = status.counts || {};
  const seasonCount = Array.isArray(status?.facets?.seasons) ? status.facets.seasons.length : 0;
  el.catalogSummary.innerHTML = [
    {
      label: "Ultima importazione",
      value: run.run_id ? `#${shortRunCode(run.run_id)}` : "--",
      note: run.finished_at ? `fine ${fmtDate(run.finished_at)}` : "run catalogo più recente",
    },
    {
      label: "Stagioni",
      value: fmtNum(seasonCount, 0),
      note: seasonCount ? `${(status.facets.seasons || []).join(", ")}` : "nessuna stagione",
    },
    {
      label: "Articoli",
      value: fmtNum(counts.articles, 0),
      note: "righe articolo (XX) nello stato corrente del catalogo",
    },
    {
      label: "Prezzi",
      value: fmtNum(counts.prices, 0),
      note: "articoli con prezzo listino/saldo importati",
    },
  ]
    .map(
      (card) => `
        <article class="catalog-summary-card">
          <span class="catalog-summary-label">${escHtml(card.label)}</span>
          <strong class="catalog-summary-value">${escHtml(card.value)}</strong>
          <span class="catalog-summary-note">${escHtml(card.note)}</span>
        </article>
      `,
    )
    .join("");
}

function catalogShowcaseFilterMeta(group) {
  return (
    {
      seasons: {
        searchKey: "seasonSearch",
        optionsEl: el.catalogShowcaseSeasonOptions,
        statusEl: el.catalogShowcaseSeasonStatus,
        allLabel: "Tutte le stagioni incluse",
        emptySearchLabel: "Nessuna stagione trovata con questo filtro.",
      },
      reparti: {
        searchKey: "repartoSearch",
        optionsEl: el.catalogShowcaseRepartoOptions,
        statusEl: el.catalogShowcaseRepartoStatus,
        allLabel: "Tutti i reparti inclusi",
        emptySearchLabel: "Nessun reparto trovato con questo filtro.",
      },
      categories: {
        searchKey: "categoriaSearch",
        optionsEl: el.catalogShowcaseCategoriaOptions,
        statusEl: el.catalogShowcaseCategoriaStatus,
        allLabel: "Tutte le categorie incluse",
        emptySearchLabel: "Nessuna categoria trovata con questo filtro.",
      },
    }[group] || null
  );
}

function normalizeCatalogShowcaseItems(items) {
  return Array.isArray(items)
    ? items.map((item) => String(item || "").trim()).filter((item) => item)
    : [];
}

function getCatalogShowcaseAvailableItems(group, facets = state.catalogStatus?.facets || {}) {
  if (group === "seasons") return normalizeCatalogShowcaseItems(facets?.seasons);
  if (group === "reparti") return normalizeCatalogShowcaseItems(facets?.reparti);
  if (group === "categories") return normalizeCatalogShowcaseItems(facets?.categorie);
  return [];
}

function pruneCatalogShowcaseSelections(facets) {
  ["seasons", "reparti", "categories"].forEach((group) => {
    const available = new Set(getCatalogShowcaseAvailableItems(group, facets));
    state.catalogShowcaseFilters[group] = (state.catalogShowcaseFilters[group] || []).filter((item) => available.has(item));
  });
}

function setCatalogShowcaseFilterStatus(group, selectedCount, totalCount) {
  const meta = catalogShowcaseFilterMeta(group);
  if (!meta?.statusEl) return;
  if (!totalCount) {
    meta.statusEl.textContent = "Nessuna voce disponibile";
    return;
  }
  if (!selectedCount) {
    meta.statusEl.textContent = meta.allLabel;
    return;
  }
  if (selectedCount === totalCount) {
    meta.statusEl.textContent = `Tutte le ${totalCount} voci selezionate`;
    return;
  }
  meta.statusEl.textContent = `${selectedCount} selezionate su ${totalCount}`;
}

function renderCatalogShowcaseFilterGroup(group, items) {
  const meta = catalogShowcaseFilterMeta(group);
  if (!meta?.optionsEl) return;
  const selected = state.catalogShowcaseFilters[group] || [];
  const selectedSet = new Set(selected);
  const searchTerm = normalizeText(state.catalogShowcaseFilters[meta.searchKey] || "");
  const visibleItems = items.filter((item) => !searchTerm || normalizeText(item).includes(searchTerm));

  setCatalogShowcaseFilterStatus(group, selected.length, items.length);

  if (!items.length) {
    meta.optionsEl.innerHTML = `<div class="catalog-filter-empty">Nessuna voce disponibile.</div>`;
    return;
  }

  if (!visibleItems.length) {
    meta.optionsEl.innerHTML = `<div class="catalog-filter-empty">${escHtml(meta.emptySearchLabel)}</div>`;
    return;
  }

  meta.optionsEl.innerHTML = visibleItems
    .map((item) => {
      const active = selectedSet.has(item);
      return `
        <button
          type="button"
          class="catalog-filter-option ${active ? "active" : ""}"
          data-showcase-filter-group="${escHtml(group)}"
          data-showcase-filter-value="${escHtml(item)}"
          aria-pressed="${active ? "true" : "false"}"
        >
          <span class="catalog-filter-option-check">${active ? "✓" : ""}</span>
          <span class="catalog-filter-option-label">${escHtml(item)}</span>
        </button>
      `;
    })
    .join("");
}

function renderCatalogShowcaseFilters(facets) {
  pruneCatalogShowcaseSelections(facets);
  renderCatalogShowcaseFilterGroup("seasons", getCatalogShowcaseAvailableItems("seasons", facets));
  renderCatalogShowcaseFilterGroup("reparti", getCatalogShowcaseAvailableItems("reparti", facets));
  renderCatalogShowcaseFilterGroup("categories", getCatalogShowcaseAvailableItems("categories", facets));
}

function toggleCatalogShowcaseFilterValue(group, value) {
  const raw = String(value || "").trim();
  if (!raw) return;
  const current = new Set(state.catalogShowcaseFilters[group] || []);
  if (current.has(raw)) current.delete(raw);
  else current.add(raw);
  state.catalogShowcaseFilters[group] = Array.from(current).sort((a, b) => a.localeCompare(b, "it"));
  renderCatalogShowcaseFilters(state.catalogStatus?.facets || {});
}

function setCatalogShowcaseFilterSelection(group, values) {
  state.catalogShowcaseFilters[group] = normalizeCatalogShowcaseItems(values).sort((a, b) => a.localeCompare(b, "it"));
  renderCatalogShowcaseFilters(state.catalogStatus?.facets || {});
}

function setCatalogShowcaseFilterSearch(group, value) {
  const meta = catalogShowcaseFilterMeta(group);
  if (!meta) return;
  state.catalogShowcaseFilters[meta.searchKey] = String(value || "");
  renderCatalogShowcaseFilters(state.catalogStatus?.facets || {});
}

function resetCatalogShowcaseFilter(group) {
  const meta = catalogShowcaseFilterMeta(group);
  if (!meta) return;
  state.catalogShowcaseFilters[group] = [];
  state.catalogShowcaseFilters[meta.searchKey] = "";
  if (group === "seasons" && el.catalogShowcaseSeasonSearch) el.catalogShowcaseSeasonSearch.value = "";
  if (group === "reparti" && el.catalogShowcaseRepartoSearch) el.catalogShowcaseRepartoSearch.value = "";
  if (group === "categories" && el.catalogShowcaseCategoriaSearch) el.catalogShowcaseCategoriaSearch.value = "";
  renderCatalogShowcaseFilters(state.catalogStatus?.facets || {});
}

function setCatalogShowcaseMessage(message, { isError = false, isHtml = false } = {}) {
  if (!el.catalogShowcaseMsg) return;
  el.catalogShowcaseMsg.classList.toggle("error-text", !!isError);
  if (isHtml) {
    el.catalogShowcaseMsg.innerHTML = message || "";
    return;
  }
  el.catalogShowcaseMsg.textContent = message || "";
}

function renderCatalogShowcaseProgress({
  visible = false,
  status = "queued",
  message = "",
  details = [],
  indeterminate = false,
  progress = null,
} = {}) {
  const statusKey = String(status || "queued").toLowerCase();
  state.catalogShowcaseLoading = visible && ["queued", "running"].includes(statusKey);
  if (el.catalogShowcaseSubmitBtn) {
    el.catalogShowcaseSubmitBtn.disabled = state.catalogShowcaseLoading;
    el.catalogShowcaseSubmitBtn.textContent = state.catalogShowcaseLoading
      ? "Generazione in corso..."
      : "Genera Catalogo Vetrina";
  }

  if (
    !el.catalogShowcaseProgressBox ||
    !el.catalogShowcaseProgressBadge ||
    !el.catalogShowcaseProgressFill ||
    !el.catalogShowcaseProgressText ||
    !el.catalogShowcaseProgressMeta
  ) {
    return;
  }

  if (!visible) {
    el.catalogShowcaseProgressBox.classList.add("hidden");
    el.catalogShowcaseProgressFill.className = "catalog-import-progress-fill";
    el.catalogShowcaseProgressFill.style.width = "0%";
    el.catalogShowcaseProgressMeta.innerHTML = "";
    return;
  }

  el.catalogShowcaseProgressBox.classList.remove("hidden");
  el.catalogShowcaseProgressBadge.textContent = catalogImportStatusLabel(statusKey);
  el.catalogShowcaseProgressBadge.className = `catalog-import-badge ${statusKey}`;

  const fillClasses = ["catalog-import-progress-fill"];
  if (indeterminate) fillClasses.push("indeterminate");
  if (statusKey === "success") fillClasses.push("success");
  if (statusKey === "failed") fillClasses.push("failed");
  el.catalogShowcaseProgressFill.className = fillClasses.join(" ");
  const pct = Number.isFinite(Number(progress)) ? Math.max(0, Math.min(100, Number(progress))) : statusKey === "success" ? 100 : 0;
  el.catalogShowcaseProgressFill.style.width = indeterminate ? "42%" : `${pct}%`;
  el.catalogShowcaseProgressText.textContent = message || "Generazione catalogo vetrina";
  el.catalogShowcaseProgressMeta.innerHTML = (Array.isArray(details) ? details : [])
    .filter(Boolean)
    .map((item) => `<span>${escHtml(item)}</span>`)
    .join("");
}

function renderCatalogFilters(facets) {
  const seasonValue = state.catalogFilters.season || "";
  const repartoValue = state.catalogFilters.reparto || "";
  const categoriaValue = state.catalogFilters.categoria || "";
  const seasons = Array.isArray(facets?.seasons) ? facets.seasons : [];
  const reparti = Array.isArray(facets?.reparti) ? facets.reparti : [];
  const categorie = Array.isArray(facets?.categorie) ? facets.categorie : [];
  if (el.catalogSeasonFilter) {
    el.catalogSeasonFilter.innerHTML =
      `<option value="">Stagione: tutte</option>` +
      seasons.map((item) => `<option value="${escHtml(item)}">${escHtml(item)}</option>`).join("");
    el.catalogSeasonFilter.value = seasons.includes(seasonValue) ? seasonValue : "";
    state.catalogFilters.season = el.catalogSeasonFilter.value || "";
  }
  if (el.catalogRepartoFilter) {
    el.catalogRepartoFilter.innerHTML =
      `<option value="">Reparto: tutti</option>` +
      reparti.map((item) => `<option value="${escHtml(item)}">${escHtml(item)}</option>`).join("");
    el.catalogRepartoFilter.value = reparti.includes(repartoValue) ? repartoValue : "";
    state.catalogFilters.reparto = el.catalogRepartoFilter.value || "";
  }
  if (el.catalogCategoriaFilter) {
    el.catalogCategoriaFilter.innerHTML =
      `<option value="">Categoria: tutte</option>` +
      categorie.map((item) => `<option value="${escHtml(item)}">${escHtml(item)}</option>`).join("");
    el.catalogCategoriaFilter.value = categorie.includes(categoriaValue) ? categoriaValue : "";
    state.catalogFilters.categoria = el.catalogCategoriaFilter.value || "";
  }
  renderCatalogShowcaseFilters(facets);
}

function renderCatalogTable() {
  if (!el.catalogTableBody) return;
  if (!state.catalogRows || state.catalogRows.length === 0) {
    el.catalogTableBody.innerHTML = `<tr class='empty-row'><td colspan='12'>Nessun articolo catalogo disponibile per i filtri selezionati.</td></tr>`;
    return;
  }
  el.catalogTableBody.innerHTML = state.catalogRows
    .map((row) => {
      const key = `${row.season_code}__${row.article_code}`;
      const selected =
        state.catalogSelected &&
        state.catalogSelected.article_code === row.article_code &&
        state.catalogSelected.season_code === row.season_code;
      return `
        <tr data-catalog-row="${escHtml(key)}" style="${selected ? "background:#edf5f0;" : ""}">
          <td>${escHtml(row.season_code || "--")}</td>
          <td>${escHtml(row.article_code || "--")}</td>
          <td>${escHtml([row.description, row.color].filter(Boolean).join(" • ") || "--")}</td>
          <td>${escHtml(row.supplier || "--")}</td>
          <td>${escHtml(row.reparto || "--")}</td>
          <td>${escHtml(row.categoria || "--")}</td>
          <td>${escHtml(fmtNum(row.giac, 0))}</td>
          <td>${escHtml(fmtNum(row.con, 0))}</td>
          <td>${escHtml(fmtNum(row.ven, 0))}</td>
          <td>${escHtml(fmtPercentValue(row.perc_ven, 1))}</td>
          <td>${escHtml(row.price_listino == null ? "--" : fmtCurrency(row.price_listino, 2))}</td>
          <td>${escHtml(row.price_saldo == null ? "--" : fmtCurrency(row.price_saldo, 2))}</td>
        </tr>
      `;
    })
    .join("");

  el.catalogTableBody.querySelectorAll("tr[data-catalog-row]").forEach((rowEl) => {
    rowEl.addEventListener("click", () => {
      const token = rowEl.dataset.catalogRow || "";
      const [season_code, article_code] = token.split("__");
      if (!season_code || !article_code) return;
      state.catalogSelected = { season_code, article_code };
      renderCatalogTable();
      refreshCatalogDetail();
    });
  });
}

function renderCatalogDetail(detail) {
  if (!el.catalogDetailBox) return;
  if (!detail) {
    el.catalogDetailBox.innerHTML = "Seleziona un articolo dalla tabella.";
    return;
  }
  if (!detail.available) {
    el.catalogDetailBox.innerHTML = `<div class="catalog-empty">${escHtml(detail.reason || "Dettaglio non disponibile.")}</div>`;
    return;
  }
  const summary = detail.summary || {};
  const stores = Array.isArray(detail.stores) ? detail.stores : [];
  const sizeKeys = [];
  stores.forEach((store) => {
    Object.keys(store.sizes || {}).forEach((size) => {
      if (!sizeKeys.includes(size)) sizeKeys.push(size);
    });
  });
  sizeKeys.sort((a, b) => Number(a) - Number(b));

  const metaHtml = `
    <div class="catalog-detail-head">
      <div>
        <div class="catalog-detail-code">${escHtml(summary.article_code || "--")}</div>
        <div class="catalog-detail-desc">${escHtml([summary.description, summary.color].filter(Boolean).join(" • ") || "--")}</div>
      </div>
      <div class="catalog-detail-tags">
        <span class="tag">${escHtml(summary.season_code || "--")}</span>
        <span class="tag">${escHtml(summary.reparto || "--")}</span>
        <span class="tag">${escHtml(summary.categoria || "--")}</span>
      </div>
    </div>
    <div class="catalog-detail-meta">
      <div><span>Fornitore</span><strong>${escHtml(summary.supplier || "--")}</strong></div>
      <div><span>Tipologia</span><strong>${escHtml(summary.tipologia || "--")}</strong></div>
      <div><span>GIAC</span><strong>${escHtml(fmtNum(summary.giac, 0))}</strong></div>
      <div><span>CON</span><strong>${escHtml(fmtNum(summary.con, 0))}</strong></div>
      <div><span>VEN</span><strong>${escHtml(fmtNum(summary.ven, 0))}</strong></div>
      <div><span>%VEN</span><strong>${escHtml(fmtPercentValue(summary.perc_ven, 1))}</strong></div>
      <div><span>Listino</span><strong>${escHtml(summary.price_listino == null ? "--" : fmtCurrency(summary.price_listino, 2))}</strong></div>
      <div><span>Saldo</span><strong>${escHtml(summary.price_saldo == null ? "--" : fmtCurrency(summary.price_saldo, 2))}</strong></div>
    </div>
  `;

  let storesHtml = "<div class='catalog-empty'>Nessun dettaglio negozio disponibile.</div>";
  if (stores.length > 0) {
    const header = ["NEG", "GIAC", "CON", "VEN", "%VEN", ...sizeKeys]
      .map((cell) => `<th>${escHtml(cell)}</th>`)
      .join("");
    const rowsHtml = stores
      .map((store) => {
        const sizeCells = sizeKeys
          .map((size) => `<td>${escHtml(fmtNum((store.sizes || {})[size] || 0, 0))}</td>`)
          .join("");
        return `
          <tr>
            <td>${escHtml(store.store_code || "--")}</td>
            <td>${escHtml(fmtNum(store.giac, 0))}</td>
            <td>${escHtml(fmtNum(store.con, 0))}</td>
            <td>${escHtml(fmtNum(store.ven, 0))}</td>
            <td>${escHtml(fmtPercentValue(store.perc_ven, 1))}</td>
            ${sizeCells}
          </tr>
        `;
      })
      .join("");
    storesHtml = `
      <div class="table-wrap">
        <table class="catalog-detail-table">
          <thead><tr>${header}</tr></thead>
          <tbody>${rowsHtml}</tbody>
        </table>
      </div>
    `;
  }

  el.catalogDetailBox.innerHTML = metaHtml + storesHtml;
}

async function refreshCatalogStatus() {
  const out = await api("/api/catalog/status");
  state.catalogStatus = out;
  renderCatalogSummary(out);
  renderCatalogFilters(out?.facets || {});
  return out;
}

async function refreshCatalogArticles() {
  const qs = new URLSearchParams({
    limit: "250",
    offset: "0",
    search: state.catalogFilters.search || "",
    season_code: state.catalogFilters.season || "",
    reparto: state.catalogFilters.reparto || "",
    categoria: state.catalogFilters.categoria || "",
  });
  const out = await api(`/api/catalog/articles?${qs.toString()}`);
  state.catalogRows = out.rows || [];
  state.catalogTotal = Number(out.total || state.catalogRows.length || 0);
  if (
    state.catalogSelected &&
    !state.catalogRows.some(
      (row) =>
        row.article_code === state.catalogSelected.article_code &&
        row.season_code === state.catalogSelected.season_code,
    )
  ) {
    state.catalogSelected = null;
  }
  renderCatalogTable();
  await refreshCatalogDetail();
}

async function refreshCatalogDetail() {
  if (!state.catalogSelected) {
    renderCatalogDetail(null);
    return;
  }
  const qs = new URLSearchParams({
    article_code: state.catalogSelected.article_code,
    season_code: state.catalogSelected.season_code,
  });
  const out = await api(`/api/catalog/article-detail?${qs.toString()}`);
  renderCatalogDetail(out);
}

async function refreshCatalog() {
  try {
    const status = await refreshCatalogStatus();
    if (!status?.available) {
      state.catalogRows = [];
      renderCatalogTable();
      renderCatalogDetail(null);
      return;
    }
    await refreshCatalogArticles();
  } catch (err) {
    renderCatalogSummary({ available: false, reason: err.message });
    state.catalogRows = [];
    renderCatalogTable();
    renderCatalogDetail({ available: false, reason: err.message });
  }
}

async function refreshActiveCatalogImportJob() {
  try {
    const out = await api("/api/catalog/import-jobs/active");
    renderCatalogImportJob(out?.job || null);
    const job = out?.job || null;
    clearCatalogImportPoller();
    if (job && job.job_id && isCatalogImportActive(job)) {
      state.catalogImportPollTimer = window.setTimeout(() => pollCatalogImportJob(job.job_id), 1200);
    } else if (job && !job.job_id && String(job.status || "").toLowerCase() === "running") {
      state.catalogImportPollTimer = window.setTimeout(refreshActiveCatalogImportJob, 2500);
    }
  } catch {
    renderCatalogImportJob(null);
  }
}

async function pollCatalogImportJob(jobId) {
  clearCatalogImportPoller();
  try {
    const job = await api(`/api/catalog/import-jobs/${encodeURIComponent(jobId)}`);
    renderCatalogImportJob(job);
    if (isCatalogImportActive(job)) {
      state.catalogImportPollTimer = window.setTimeout(() => pollCatalogImportJob(jobId), 1200);
      return;
    }
    if (String(job.status || "").toLowerCase() === "success") {
      clearCatalogQueuedFiles();
      setCatalogImportMessage(
        `Import catalogo completato #${shortRunCode(job.run_id)}. Excel ${job.classification?.excel_count || 0}, CSV ${job.classification?.price_count || 0}.`,
      );
      await refreshCatalog();
      return;
    }
    if (String(job.status || "").toLowerCase() === "failed") {
      setCatalogImportMessage(`Errore import catalogo: ${job.error || job.message || "errore sconosciuto"}`, { isError: true });
      return;
    }
  } catch (err) {
    setCatalogImportMessage(`Errore stato import catalogo: ${err.message}`, { isError: true });
    renderCatalogImportJob(null);
  }
}

async function refreshActiveCatalogShowcaseJob() {
  try {
    const out = await api("/api/catalog/showcase/jobs/active");
    const job = out?.job || null;
    if (job) {
      renderCatalogShowcaseJob(job);
    } else {
      const latest = await refreshLatestCatalogShowcaseJob();
      if (!latest && (!state.catalogShowcaseJob || isCatalogShowcaseActive(state.catalogShowcaseJob))) {
        renderCatalogShowcaseJob(null);
      }
    }
    clearCatalogShowcasePoller();
    if (job && job.job_id && isCatalogShowcaseActive(job)) {
      state.catalogShowcasePollTimer = window.setTimeout(() => pollCatalogShowcaseJob(job.job_id), 900);
    }
  } catch (err) {
    if (isCatalogShowcaseActive()) {
      setCatalogShowcaseMessage(`Errore stato catalogo vetrina: ${err.message}`, { isError: true });
    }
  }
}

async function pollCatalogShowcaseJob(jobId) {
  clearCatalogShowcasePoller();
  try {
    const job = await api(`/api/catalog/showcase/jobs/${encodeURIComponent(jobId)}`);
    renderCatalogShowcaseJob(job);
    if (isCatalogShowcaseActive(job)) {
      state.catalogShowcasePollTimer = window.setTimeout(() => pollCatalogShowcaseJob(jobId), 900);
      return;
    }

    if (String(job?.status || "").toLowerCase() === "success") {
      renderCatalogShowcaseTerminalMessage(job);
      return;
    }
    if (String(job?.status || "").toLowerCase() === "failed") {
      renderCatalogShowcaseTerminalMessage(job);
    }
  } catch (err) {
    const latest = await refreshLatestCatalogShowcaseJob();
    if (latest) return;
    setCatalogShowcaseMessage(`Errore stato catalogo vetrina: ${err.message}`, { isError: true });
  }
}

async function startCatalogImport(evt) {
  evt.preventDefault();
  if (isCatalogImportActive()) {
    setCatalogImportMessage("Esiste gia' un import catalogo in corso.", { isError: true });
    return;
  }
  const queuedFiles = (state.catalogQueuedFiles || []).map((item) => item.file).filter(Boolean);
  const hasFiles = queuedFiles.length > 0;
  if (!hasFiles) {
    setCatalogImportMessage("Carica almeno un file catalogo (.xls/.xlsx/.csv).", { isError: true });
    return;
  }
  const fd = new FormData();
  queuedFiles.forEach((file) => fd.append("files", file));
  fd.append("sheet", el.catalogSheet?.value?.trim() || "Situazione Articoli");
  fd.append("create_schema", el.catalogCreateSchema?.checked ? "true" : "false");

  setCatalogImportMessage("Import catalogo in corso...");
  try {
    const res = await fetch("/api/catalog/import", {
      method: "POST",
      body: fd,
    });
    const txt = await res.text();
    let payload = {};
    try {
      payload = txt ? JSON.parse(txt) : {};
    } catch {
      payload = { raw: txt };
    }
    if (!res.ok) {
      throw new Error(payload.detail || payload.raw || `HTTP ${res.status}`);
    }
    renderCatalogImportJob(payload?.job || null);
    const job = payload?.job || null;
    const ignoredCount = Number(job?.classification?.ignored_count || 0);
    const ignoredMsg = ignoredCount ? `, ignorati ${ignoredCount}` : "";
    setCatalogImportMessage(`Import catalogo avviato. Excel ${job?.classification?.excel_count || 0}, CSV ${job?.classification?.price_count || 0}${ignoredMsg}.`);
    if (job?.job_id) {
      await pollCatalogImportJob(job.job_id);
    }
  } catch (err) {
    setCatalogImportMessage(`Errore import catalogo: ${err.message}`, { isError: true });
  }
}

async function startCatalogShowcaseExport(evt) {
  evt.preventDefault();
  if (isCatalogShowcaseActive()) {
    setCatalogShowcaseMessage("Generazione catalogo vetrina gia' in corso...");
    return;
  }
  if (!state.catalogStatus?.available) {
    setCatalogShowcaseMessage("Importa prima il catalogo nel database.", { isError: true });
    return;
  }

  const payload = {
    run_id: null,
    export_mode: el.catalogShowcaseExportMode?.value || "both",
    primary_source: el.catalogShowcasePrimarySource?.value || "local",
    allow_fallback: !!el.catalogShowcaseFallback?.checked,
    selected_seasons: state.catalogShowcaseFilters.seasons || [],
    selected_reparti: state.catalogShowcaseFilters.reparti || [],
    selected_categories: state.catalogShowcaseFilters.categories || [],
    manual_codes_text: el.catalogShowcaseManualCodes?.value || "",
    photo_root: el.catalogPhotoRoot?.value?.trim() || "",
    photo_position: el.catalogShowcasePhotoPosition?.value?.trim() || "xl",
    allow_position_variants: !!el.catalogShowcaseAllowVariants?.checked,
  };

  const filtersSummary = [
    payload.selected_seasons.length ? `${payload.selected_seasons.length} stagioni` : "tutte le stagioni",
    payload.selected_reparti.length ? `${payload.selected_reparti.length} reparti` : "tutti i reparti",
    payload.selected_categories.length ? `${payload.selected_categories.length} categorie` : "tutte le categorie",
  ].join(" • ");

  setCatalogShowcaseMessage("Generazione catalogo vetrina avviata...");
  renderCatalogShowcaseProgress({
    visible: true,
    status: "queued",
    message: "Catalogo vetrina in coda...",
    details: [
      filtersSummary,
      payload.primary_source === "local" ? "Sorgente foto: archivio locale" : "Sorgente foto: sito BARCA",
      `Formato: ${String(payload.export_mode || "both").toUpperCase()}`,
    ],
    indeterminate: false,
    progress: 0,
  });
  try {
    const out = await api("/api/catalog/showcase/jobs", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    const job = out?.job || null;
    renderCatalogShowcaseJob(job);
    if (job?.job_id) {
      await pollCatalogShowcaseJob(job.job_id);
      return;
    }
    throw new Error("Job catalogo vetrina non restituito dal server.");
  } catch (err) {
    renderCatalogShowcaseProgress({
      visible: true,
      status: "failed",
      message: "Errore durante la generazione del catalogo vetrina.",
      details: [err.message || "errore sconosciuto"],
      indeterminate: false,
      progress: 100,
    });
    setCatalogShowcaseMessage(`Errore catalogo vetrina: ${err.message}`, { isError: true });
  }
}

async function loadDashboardRuns() {
  try {
    const out = await api("/api/dashboard/runs?limit=200");
    state.dashboardRuns = out.runs || [];
    if (state.dashboardRuns.length === 0) {
      if (el.dashboardRunSelect) {
        el.dashboardRunSelect.innerHTML = "<option value=''>Nessun aggiornamento disponibile</option>";
      }
      state.dashboardRunId = null;
      return;
    }
    if (!state.dashboardRunId || !state.dashboardRuns.some((r) => r.run_id === state.dashboardRunId)) {
      state.dashboardRunId = state.dashboardRuns[0].run_id;
    }
    renderActiveRunText();
    if (el.dashboardRunSelect) {
      el.dashboardRunSelect.innerHTML = state.dashboardRuns
        .map((r) => {
          const selected = r.run_id === state.dashboardRunId ? "selected" : "";
          return `<option value="${escHtml(r.run_id)}" ${selected}>${escHtml(dashboardRunLabel(r))}</option>`;
        })
        .join("");
    }
  } catch (err) {
    state.dashboardRuns = [];
    state.dashboardRunId = null;
    if (el.dashboardRunSelect) {
      el.dashboardRunSelect.innerHTML = "<option value=''>Errore caricamento aggiornamenti dashboard</option>";
    }
    setDashboardWarn(`Errore elenco aggiornamenti dashboard: ${err.message}`);
  }
}

async function refreshDashboard() {
  try {
    if (state.dashboardRuns.length === 0) {
      await loadDashboardRuns();
    }
    const runId = state.dashboardRunId || "";
    const qs = runId ? `?run_id=${encodeURIComponent(runId)}&table_limit=200` : "?table_limit=200";
    const out = await api(`/api/dashboard${qs}`);

    if (!out.connected) {
      setDashboardWarn(`Dashboard non disponibile: ${out.reason || "errore connessione DB"}`);
      state.dashboardData = { tables: {} };
      renderRunContextPills(null, null);
      renderDashboardLegend();
      renderDashboardQuickFacts({});
      renderDashboardKpis({}, {});
      renderInsights({});
      renderBarChart(el.chartTransferTo, [], { unit: "pairs" });
      renderBarChart(el.chartTransferFrom, [], { unit: "pairs" });
      renderBarChart(el.chartTransferReason, [], { unit: "pairs" });
      renderBarChart(el.chartOrdersSeasonMode, [], { unit: "pairs" });
      renderBarChart(el.chartOrdersModule, [], { unit: "pairs" });
      renderBarChart(el.chartOrdersMode, [], { unit: "pairs" });
      renderBarChart(el.chartOrdersPriceBand, [], { unit: "pairs" });
      renderBarChart(el.chartCriticalByShop, [], { unit: "pairs" });
      renderBarChart(el.chartNextCurrentCategory, [], { unit: "pairs" });
      renderBarChart(el.chartNextCurrentDeltaCategory, [], { unit: "pairs" });
      renderBarChart(el.chartNextCurrentPriceBand, [], { unit: "pairs" });
      renderAllDashboardTables();
      if (el.dashboardSubtitle) {
        el.dashboardSubtitle.textContent = "Connessione DB non disponibile.";
      }
      return;
    }

    if (!out.run) {
      setDashboardWarn(out.reason || "Nessun aggiornamento disponibile.");
      state.dashboardData = { tables: {} };
      renderRunContextPills(null, null);
      renderDashboardLegend();
      renderDashboardQuickFacts({});
      renderDashboardKpis({}, {});
      renderInsights({});
      renderBarChart(el.chartTransferTo, [], { unit: "pairs" });
      renderBarChart(el.chartTransferFrom, [], { unit: "pairs" });
      renderBarChart(el.chartTransferReason, [], { unit: "pairs" });
      renderBarChart(el.chartOrdersSeasonMode, [], { unit: "pairs" });
      renderBarChart(el.chartOrdersModule, [], { unit: "pairs" });
      renderBarChart(el.chartOrdersMode, [], { unit: "pairs" });
      renderBarChart(el.chartOrdersPriceBand, [], { unit: "pairs" });
      renderBarChart(el.chartCriticalByShop, [], { unit: "pairs" });
      renderBarChart(el.chartNextCurrentCategory, [], { unit: "pairs" });
      renderBarChart(el.chartNextCurrentDeltaCategory, [], { unit: "pairs" });
      renderBarChart(el.chartNextCurrentPriceBand, [], { unit: "pairs" });
      renderAllDashboardTables();
      if (el.dashboardSubtitle) {
        el.dashboardSubtitle.textContent = "Nessun aggiornamento caricabile per dashboard.";
      }
      return;
    }

    setDashboardWarn("");
    state.dashboardData = out;
    renderRunContextPills(out.run, out.baseline_run || null);
    if (el.dashboardSubtitle) {
      const runLabel = runTypeLabel(out.run);
      const runContext = runContextSummary(out.run, { includeMethods: false, fallbackDefault: false });
      const runStatus = out.run.status_label || out.run.status_raw || out.run.status || "n/d";
      const subtitleParts = [runLabel];
      if (runContext) subtitleParts.push(runContext);
      subtitleParts.push(`stato ${runStatus}`);
      subtitleParts.push(`inizio ${fmtDateCompact(out.run.started_at)}`);
      if (out.run.finished_at) subtitleParts.push(`fine ${fmtDateCompact(out.run.finished_at)}`);
      if (out.baseline_run?.run_id) {
        const baseContext = runContextSummary(out.baseline_run, { includeMethods: false, fallbackDefault: false });
        subtitleParts.push(`confronto con ${baseContext || `#${shortRunCode(out.baseline_run.run_id)}`}`);
      }
      el.dashboardSubtitle.textContent = subtitleParts.join(" · ");
    }
    renderDashboardLegend();
    renderDashboardQuickFacts(out.kpis || {});
    renderDashboardKpis(out.kpis || {}, out.kpi_deltas || {});
    renderInsights(out.kpis || {});
    renderBarChart(el.chartTransferTo, out.charts?.transfer_to || [], { unit: "pairs", digits: 2 });
    renderBarChart(el.chartTransferFrom, out.charts?.transfer_from || [], { unit: "pairs", digits: 2 });
    renderBarChart(el.chartTransferReason, out.charts?.transfer_reason || [], { unit: "pairs", digits: 2 });
    renderBarChart(el.chartOrdersSeasonMode, out.charts?.orders_by_season_mode || [], { unit: "pairs", digits: 2 });
    renderBarChart(el.chartOrdersModule, out.charts?.orders_by_module || [], { unit: "pairs", digits: 2 });
    renderBarChart(el.chartOrdersMode, out.charts?.orders_by_mode || [], { unit: "pairs", digits: 2 });
    renderBarChart(el.chartOrdersPriceBand, out.charts?.orders_by_price_band || [], { unit: "pairs", digits: 2 });
    renderBarChart(el.chartCriticalByShop, out.charts?.critical_by_shop || [], { unit: "pairs", digits: 2 });
    renderBarChart(el.chartNextCurrentCategory, out.charts?.next_current_by_category || [], { unit: "pairs", digits: 2 });
    renderBarChart(el.chartNextCurrentDeltaCategory, out.charts?.next_current_delta_positive_by_category || [], { unit: "pairs", digits: 2 });
    renderBarChart(el.chartNextCurrentPriceBand, out.charts?.next_current_by_price_band || [], { unit: "pairs", digits: 2 });
    renderAllDashboardTables();
  } catch (err) {
    setDashboardWarn(`Errore dashboard: ${err.message}`);
  }
}

async function stopRun(runId) {
  try {
    await api(`/api/runs/${runId}/stop`, { method: "POST" });
    await refreshRuns();
  } catch (err) {
    alert(`Stop fallito: ${err.message}`);
  }
}

async function refreshRuns() {
  try {
    const out = await api(`/api/runs?${buildRunsQuery()}`);
    state.runs = out.runs || [];
    state.runsTotal = Number(out.total || state.runs.length || 0);
    state.runsOffset = Number(out.offset || 0);
    state.runsLimit = Number(out.limit || state.runsLimit || 40);
    renderActiveRunText();
    if (state.selectedRunId && !state.runs.some((r) => r.run_id === state.selectedRunId)) {
      state.selectedRunId = null;
    }
    if (!state.selectedRunId && state.runs.length > 0) {
      state.selectedRunId = state.runs[0].run_id;
    }

    const rowsHtml = state.runs
      .map((r) => {
        const isSelected = r.run_id === state.selectedRunId;
        const stopBtn =
          r.can_stop
            ? `<button class="btn warn" data-stop="${r.run_id}">Stop</button>`
            : "";
        const shortRunId = String(r.run_id || "--");
        const runType = runTypeLabel(r);
        const runContext = runContextSummary(r, { includeMethods: false });
        const runIdSafe = escHtml(fmt(r.run_id));
        const runTypeSafe = escHtml(runType);
        return `
          <tr data-run="${r.run_id}" style="${isSelected ? "background:#edf5f0;" : ""}">
            <td title="${runIdSafe}">
              <div class="run-main">
                <span class="run-title">#${escHtml(shortRunId.length > 12 ? shortRunId.slice(0, 8) : shortRunId)}</span>
                <span class="run-subtitle">${escHtml(fmtDate(r.started_at || r.created_at))}</span>
              </div>
            </td>
            <td>${sourceBadge(r.source)}</td>
            <td class="type-cell" title="${runTypeSafe}">${runTypeSafe}</td>
            <td class="context-cell" title="${escHtml(runContext)}">${escHtml(runContext)}</td>
            <td>${badge(r.status, r.status_label || r.status_raw)}</td>
            <td>${escHtml(fmtDate(r.started_at || r.created_at))}</td>
            <td>${escHtml(fmtDate(r.ended_at))}</td>
            <td>${escHtml(fmt(r.return_code))}</td>
            <td>${stopBtn}</td>
          </tr>
        `;
      })
      .join("");
    el.runsTableBody.innerHTML = rowsHtml || "<tr class='empty-row'><td colspan='9'>Nessun aggiornamento per i filtri selezionati.</td></tr>";
    renderRunsPager();

    el.runsTableBody.querySelectorAll("tr[data-run]").forEach((row) => {
      row.addEventListener("click", () => {
        state.selectedRunId = row.dataset.run;
        refreshRuns();
        refreshSelectedRunDetails();
      });
    });

    el.runsTableBody.querySelectorAll("button[data-stop]").forEach((btn) => {
      btn.addEventListener("click", (evt) => {
        evt.stopPropagation();
        stopRun(btn.dataset.stop);
      });
    });
  } catch (err) {
    el.runsTableBody.innerHTML = `<tr><td colspan='9'>Errore: ${err.message}</td></tr>`;
    state.runsTotal = 0;
    renderRunsPager();
  }
}

async function refreshSelectedRunDetails() {
  if (!state.selectedRunId) {
    el.selectedRunBox.textContent = "Seleziona un aggiornamento dalla tabella.";
    if (state.developerMode) {
      el.logsBox.textContent = "Seleziona un aggiornamento avviato da questa interfaccia per vedere i log raw.";
    }
    return;
  }
  try {
    const run = await api(`/api/runs/${state.selectedRunId}`);
    el.selectedRunBox.textContent = JSON.stringify(run, null, 2);
    if (!state.developerMode) return;
    const selected = state.runs.find((r) => r.run_id === state.selectedRunId);
    if (selected && selected.source !== "ui") {
      el.logsBox.textContent = "Log raw disponibili solo per aggiornamenti avviati da questa interfaccia.";
      return;
    }
    const logs = await api(`/api/runs/${state.selectedRunId}/logs?tail=400`);
    el.logsBox.textContent = (logs.lines || []).join("\n");
  } catch (err) {
    el.selectedRunBox.textContent = `Errore: ${err.message}`;
    if (state.developerMode) {
      el.logsBox.textContent = `Errore logs: ${err.message}`;
    }
  }
}

async function refreshAll(includeDashboard = false) {
  await Promise.all([
    refreshHealth(),
    refreshDb(),
    refreshOutputs(),
    refreshRuns(),
    refreshCatalog(),
    refreshActiveCatalogImportJob(),
    refreshActiveCatalogShowcaseJob(),
  ]);
  await refreshSelectedRunDetails();
  if (includeDashboard) {
    await loadDashboardRuns();
    await refreshDashboard();
  }
}

function initDashboardTableControls() {
  Object.keys(DASHBOARD_TABLE_CONFIG).forEach((tableKey) => {
    const cfg = DASHBOARD_TABLE_CONFIG[tableKey];
    cfg.tableEl?.querySelectorAll("th.sortable").forEach((th) => {
      th.addEventListener("click", () => {
        const key = th.dataset.key;
        if (!key) return;
        const tState = state.dashboardTableState[tableKey];
        if (tState.sortKey === key) {
          tState.sortDir = tState.sortDir === "desc" ? "asc" : "desc";
        } else {
          tState.sortKey = key;
          tState.sortDir = cfg.numericColumns.includes(key) ? "desc" : "asc";
        }
        renderDashboardTableByKey(tableKey);
      });
    });

    cfg.searchEl?.addEventListener("input", () => {
      state.dashboardTableState[tableKey].search = cfg.searchEl.value || "";
      renderDashboardTableByKey(tableKey);
    });

    cfg.rowLimitEl?.addEventListener("change", () => {
      const n = Number(cfg.rowLimitEl.value || "20");
      state.dashboardTableState[tableKey].rowLimit = Number.isFinite(n) && n > 0 ? n : 20;
      state.dashboardTableState[tableKey].showAll = false;
      renderDashboardTableByKey(tableKey);
    });

    cfg.showAllEl?.addEventListener("click", () => {
      const tState = state.dashboardTableState[tableKey];
      tState.showAll = !tState.showAll;
      renderDashboardTableByKey(tableKey);
    });

    cfg.focusEl?.addEventListener("click", () => {
      toggleTableFullscreen(tableKey, null);
    });

    cfg.exportCsvEl?.addEventListener("click", () => exportDashboardTableCsv(tableKey));

    cfg.exportXlsxEl?.addEventListener("click", async () => {
      try {
        await exportDashboardTableXlsx(tableKey);
      } catch (err) {
        alert(`Export Excel fallito: ${err.message}`);
      }
    });

    if (cfg.rowLimitEl) {
      cfg.rowLimitEl.value = String(state.dashboardTableState[tableKey].rowLimit || 20);
    }
  });
}

function initEvents() {
  const limitVal = Number(el.runsPageSize?.value || "40");
  state.runsLimit = Number.isFinite(limitVal) && limitVal > 0 ? limitVal : 40;
  initDashboardTableControls();
  bindCatalogDropzone();
  el.refreshBtn.addEventListener("click", () => refreshAll(true));
  el.devModeBtn.addEventListener("click", toggleDeveloperMode);
  el.runForm.addEventListener("submit", startRun);
  el.catalogImportForm?.addEventListener("submit", startCatalogImport);
  el.catalogFiles?.addEventListener("change", handleCatalogFileSelection);
  el.catalogClearFilesBtn?.addEventListener("click", () => clearCatalogQueuedFiles({ clearMessage: true }));
  el.catalogFileQueue?.addEventListener("click", (evt) => {
    const removeBtn = evt.target?.closest?.("[data-remove-catalog-file]");
    if (!removeBtn) return;
    const fileId = removeBtn.getAttribute("data-remove-catalog-file");
    if (!fileId) return;
    removeCatalogQueuedFile(fileId);
  });
  el.catalogShowcaseForm?.addEventListener("submit", startCatalogShowcaseExport);
  el.catalogShowcaseSeasonSearch?.addEventListener("input", () =>
    setCatalogShowcaseFilterSearch("seasons", el.catalogShowcaseSeasonSearch.value || ""),
  );
  el.catalogShowcaseRepartoSearch?.addEventListener("input", () =>
    setCatalogShowcaseFilterSearch("reparti", el.catalogShowcaseRepartoSearch.value || ""),
  );
  el.catalogShowcaseCategoriaSearch?.addEventListener("input", () =>
    setCatalogShowcaseFilterSearch("categories", el.catalogShowcaseCategoriaSearch.value || ""),
  );
  el.catalogShowcaseSeasonAll?.addEventListener("click", () =>
    setCatalogShowcaseFilterSelection("seasons", getCatalogShowcaseAvailableItems("seasons")),
  );
  el.catalogShowcaseRepartoAll?.addEventListener("click", () =>
    setCatalogShowcaseFilterSelection("reparti", getCatalogShowcaseAvailableItems("reparti")),
  );
  el.catalogShowcaseCategoriaAll?.addEventListener("click", () =>
    setCatalogShowcaseFilterSelection("categories", getCatalogShowcaseAvailableItems("categories")),
  );
  el.catalogShowcaseSeasonClear?.addEventListener("click", () => resetCatalogShowcaseFilter("seasons"));
  el.catalogShowcaseRepartoClear?.addEventListener("click", () => resetCatalogShowcaseFilter("reparti"));
  el.catalogShowcaseCategoriaClear?.addEventListener("click", () => resetCatalogShowcaseFilter("categories"));
  [el.catalogShowcaseSeasonOptions, el.catalogShowcaseRepartoOptions, el.catalogShowcaseCategoriaOptions].forEach(
    (container) => {
      container?.addEventListener("click", (evt) => {
        const option = evt.target?.closest?.("[data-showcase-filter-group][data-showcase-filter-value]");
        if (!option) return;
        toggleCatalogShowcaseFilterValue(
          option.getAttribute("data-showcase-filter-group"),
          option.getAttribute("data-showcase-filter-value"),
        );
      });
    },
  );
  el.catalogSaveSettingsBtn?.addEventListener("click", saveCatalogSettings);
  el.viewTabs.forEach((btn) => {
    btn.addEventListener("click", () => setActiveView(btn.dataset.viewTarget));
  });
  el.dashSectionTabs.forEach((btn) => {
    btn.addEventListener("click", () => setActiveDashSection(btn.dataset.dashTarget));
  });
  el.runsSearch?.addEventListener("input", handleRunsFilterChanged);
  el.runsSourceFilter?.addEventListener("change", handleRunsFilterChanged);
  el.runsStatusFilter?.addEventListener("change", handleRunsFilterChanged);
  el.runsSortBy?.addEventListener("change", handleRunsFilterChanged);
  el.runsSortDir?.addEventListener("change", handleRunsFilterChanged);
  el.runsPageSize?.addEventListener("change", handleRunsFilterChanged);
  el.runsTypeFilter?.addEventListener("input", handleRunsFilterChanged);
  el.runsClearFiltersBtn?.addEventListener("click", () => {
    state.runsOffset = 0;
    if (el.runsSearch) el.runsSearch.value = "";
    if (el.runsSourceFilter) el.runsSourceFilter.value = "all";
    if (el.runsStatusFilter) el.runsStatusFilter.value = "";
    if (el.runsSortBy) el.runsSortBy.value = "started_at";
    if (el.runsSortDir) el.runsSortDir.value = "desc";
    if (el.runsPageSize) el.runsPageSize.value = "40";
    state.runsLimit = 40;
    if (el.runsTypeFilter) el.runsTypeFilter.value = "";
    refreshRuns();
    refreshSelectedRunDetails();
  });
  el.runsPrevPageBtn?.addEventListener("click", () => {
    if (state.runsOffset <= 0) return;
    state.runsOffset = Math.max(0, state.runsOffset - state.runsLimit);
    refreshRuns();
    refreshSelectedRunDetails();
  });
  el.runsNextPageBtn?.addEventListener("click", () => {
    if (state.runsOffset + state.runsLimit >= state.runsTotal) return;
    state.runsOffset += state.runsLimit;
    refreshRuns();
    refreshSelectedRunDetails();
  });
  el.dashboardRunSelect?.addEventListener("change", () => {
    state.dashboardRunId = el.dashboardRunSelect?.value || null;
    refreshDashboard();
  });
  el.dashboardRefreshBtn?.addEventListener("click", async () => {
    await loadDashboardRuns();
    await refreshDashboard();
  });
  el.catalogRefreshBtn?.addEventListener("click", refreshCatalog);
  el.catalogSearch?.addEventListener("input", () => {
    state.catalogFilters.search = el.catalogSearch.value || "";
    refreshCatalogArticles();
  });
  el.catalogSeasonFilter?.addEventListener("change", () => {
    state.catalogFilters.season = el.catalogSeasonFilter.value || "";
    refreshCatalogArticles();
  });
  el.catalogRepartoFilter?.addEventListener("change", () => {
    state.catalogFilters.reparto = el.catalogRepartoFilter.value || "";
    refreshCatalogArticles();
  });
  el.catalogCategoriaFilter?.addEventListener("change", () => {
    state.catalogFilters.categoria = el.catalogCategoriaFilter.value || "";
    refreshCatalogArticles();
  });
  window.addEventListener("keydown", (evt) => {
    if (evt.key === "Escape" && state.fullscreenTableKey) {
      toggleTableFullscreen(state.fullscreenTableKey, false);
    }
  });
}

async function init() {
  initEvents();
  renderCatalogQueuedFiles();
  renderCatalogImportJob(null);
  renderCatalogShowcaseJob(null);
  renderDashboardLegend();
  renderDashboardQuickFacts({});
  renderDashboardKpis({}, {});
  renderAllDashboardTables();
  renderInsights({});
  setActiveDashSection(state.activeDashSection);
  setActiveView(state.activeView);
  await loadSettings();
  await refreshAll(true);
  setInterval(() => refreshAll(state.activeView === "dashboard"), 5000);
}

init();
