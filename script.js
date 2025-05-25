let selectedDataType = null;
let selectedRegion = null;
let countries = [];
let dateRange = [null, null];
let allData = [];  // all parsed CSV rows as JS objects
let filteredPreview = [];

// Map your data type + region to ZIP file URLs on your GitHub Pages site
const zipFiles = {
  "Temporal_AFRO": "data/Temporal_extract_AFRO_V1_3.zip",
  "Temporal_EMRO": "data/Temporal_extract_EMRO_V1_3.zip",
  "Temporal_EURO": "data/Temporal_extract_EURO_V1_3.zip",
  "Temporal_PAHO": "data/Temporal_extract_PAHO_V1_3.zip",
  "Temporal_SEARO": "data/Temporal_extract_SEARO_V1_3.zip",
  "Temporal_WPRO": "data/Temporal_extract_WPRO_V1_3.zip",

  "Spatial_AFRO": "data/Spatial_extract_AFRO_V1_3.zip",
  "Spatial_EMRO": "data/Spatial_extract_EMRO_V1_3.zip",
  "Spatial_EURO": "data/Spatial_extract_EURO_V1_3.zip",
  "Spatial_PAHO": "data/Spatial_extract_PAHO_V1_3.zip",
  "Spatial_SEARO": "data/Spatial_extract_SEARO_V1_3.zip",
  "Spatial_WPRO": "data/Spatial_extract_WPRO_V1_3.zip",

  "National_AFRO": "data/National_extract_AFRO_V1_3.zip",
  "National_EMRO": "data/National_extract_EMRO_V1_3.zip",
  "National_EURO": "data/National_extract_EURO_V1_3.zip",
  "National_PAHO": "data/National_extract_PAHO_V1_3.zip",
  "National_SEARO": "data/National_extract_SEARO_V1_3.zip",
  "National_WPRO": "data/National_extract_WPRO_V1_3.zip"
};

// Util: convert filtered data array to CSV string
function convertToCSV(data) {
  return Papa.unparse(data);
}

// Util: download CSV file from string content
function downloadCSV(csvString, filename) {
  const blob = new Blob([csvString], { type: 'text/csv;charset=utf-8;' });
  const link = document.createElement("a");
  const url = URL.createObjectURL(blob);
  link.setAttribute("href", url);
  link.setAttribute("download", filename);
  link.style.visibility = 'hidden';
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

// Load and parse ZIP file CSV
async function loadAndParseZip(dataType, region) {
  const key = `${dataType}_${region}`;
  if (!zipFiles[key]) {
    alert("No ZIP file URL for data type & region: " + key);
    return [];
  }
  const url = zipFiles[key];
  console.log(`Fetching ZIP: ${url}`);

  try {
    const response = await fetch(url);
    if (!response.ok) {
      alert("Failed to fetch ZIP file.");
      return [];
    }

    const arrayBuffer = await response.arrayBuffer();
    const zip = await JSZip.loadAsync(arrayBuffer);

    // Find first CSV file inside ZIP
    const csvFileName = Object.keys(zip.files).find(name => name.endsWith(".csv"));
    if (!csvFileName) {
      alert("No CSV file found in ZIP.");
      return [];
    }

    const csvText = await zip.file(csvFileName).async("text");
    console.log(`Parsed CSV from ZIP: ${csvFileName}`);

    // Parse CSV to JSON array
    const parsed = Papa.parse(csvText, { header: true, skipEmptyLines: true });
    if (parsed.errors.length) {
      console.warn("CSV parse errors:", parsed.errors);
    }

    return parsed.data;
  } catch (error) {
    console.error("Error loading or parsing ZIP:", error);
    alert("Error loading or parsing ZIP file.");
    return [];
  }
}

// Filter data by selected countries and date range
function filterData(data, countries, startDate, endDate) {
  if (!countries.length || !startDate || !endDate) return [];

  const start = new Date(startDate);
  const end = new Date(endDate);

  return data.filter(row => {
    if (!row.adm_0_name || !row.calendar_start_date || !row.calendar_end_date) return false;

    const rowCountry = row.adm_0_name;
    if (!countries.includes(rowCountry)) return false;

    const rowStart = new Date(row.calendar_start_date);
    const rowEnd = new Date(row.calendar_end_date);

    // Check if row date range overlaps with selected date range
    return (rowStart <= end) && (rowEnd >= start);
  });
}

// Render preview table with filtered data
function renderPreviewTable(data) {
  if ($.fn.dataTable.isDataTable('#previewTable')) {
    $('#previewTable').DataTable().clear().destroy();
  }

  $('#previewTable').DataTable({
    data: data,
    columns: [
      { title: "Country", data: "adm_0_name" },
      { title: "Date Start", data: "calendar_start_date" },
      { title: "Date End", data: "calendar_end_date" },
      { title: "Temporal Res", data: "T_res" },
      { title: "Spatial Res", data: "S_res" },
      { title: "Value", data: "dengue_total" }
    ],
    pageLength: 10,
    lengthChange: false,
    searching: false
  });
}

// Populate country select options based on loaded data
function updateCountryOptions(data) {
  const countrySelect = document.getElementById("countrySelect");
  const uniqueCountries = [...new Set(data.map(row => row.adm_0_name))].sort();

  countrySelect.innerHTML = "";
  uniqueCountries.forEach(c => {
    const option = document.createElement("option");
    option.value = c;
    option.text = c;
    countrySelect.appendChild(option);
  });

  // Clear selection
  countrySelect.value = null;
  $(countrySelect).trigger("change");
}

// Compute min and max date range from data for selected countries
function computeDateRange(data, countries) {
  const filtered = data.filter(row => countries.includes(row.adm_0_name));
  const datesStart = filtered.map(row => new Date(row.calendar_start_date));
  const datesEnd = filtered.map(row => new Date(row.calendar_end_date));

  if (!datesStart.length) return [null, null];
  return [
    new Date(Math.min(...datesStart)).toISOString().substring(0, 10),
    new Date(Math.max(...datesEnd)).toISOString().substring(0, 10)
  ];
}

// Event handlers
document.getElementById("dataTypeSelect").addEventListener("change", async (e) => {
  selectedDataType = e.target.value;
  if (selectedDataType && selectedRegion) {
    allData = await loadAndParseZip(selectedDataType, selectedRegion);
    updateCountryOptions(allData);
  }
});

document.getElementById("regionSelect").addEventListener("change", async (e) => {
  selectedRegion = e.target.value;
  if (selectedDataType && selectedRegion) {
    allData = await loadAndParseZip(selectedDataType, selectedRegion);
    updateCountryOptions(allData);
  }
});

document.getElementById("filterBtn").addEventListener("click", () => {
  const countrySelect = document.getElementById("countrySelect");
  const selectedCountries = Array.from(countrySelect.selectedOptions).map(o => o.value);

  const startDate = document.getElementById("startDate").value;
  const endDate = document.getElementById("endDate").value;

  filteredPreview = filterData(allData, selectedCountries, startDate, endDate);
  renderPreviewTable(filteredPreview);
});

document.getElementById("downloadBtn").addEventListener("click", () => {
  if (!filteredPreview.length) {
    alert("No data to download. Please preview filtered data first.");
    return;
  }
  const csvString = convertToCSV(filteredPreview);
  downloadCSV(csvString, "filtered_data.csv");
});
