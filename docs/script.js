// Global vars
let selectedDataType = null;
let selectedRegion = null;
let allData = [];
let filteredPreview = [];
let dateRange = [null, null];

// Map dataType + region to zip file URL (adjust if your resources/ folder structure changes)
const zipFiles = {
  "National_AFRO": "resources/National_extract_AFRO_V1_3.zip",
  "National_EMRO": "resources/National_extract_EMRO_V1_3.zip",
  "National_EURO": "resources/National_extract_EURO_V1_3.zip",
  "National_PAHO": "resources/National_extract_PAHO_V1_3.zip",
  "National_SEARO": "resources/National_extract_SEARO_V1_3.zip",
  "National_WPRO": "resources/National_extract_WPRO_V1_3.zip",
  "Spatial_AFRO": "resources/Spatial_extract_AFRO_V1_3.zip",
  "Spatial_EMRO": "resources/Spatial_extract_EMRO_V1_3.zip",
  "Spatial_EURO": "resources/Spatial_extract_EURO_V1_3.zip",
  "Spatial_PAHO": "resources/Spatial_extract_PAHO_V1_3.zip",
  "Spatial_SEARO": "resources/Spatial_extract_SEARO_V1_3.zip",
  "Spatial_WPRO": "resources/Spatial_extract_WPRO_V1_3.zip",
  "Temporal_AFRO": "resources/Temporal_extract_AFRO_V1_3.zip",
  "Temporal_EMRO": "resources/Temporal_extract_EMRO_V1_3.zip",
  "Temporal_EURO": "resources/Temporal_extract_EURO_V1_3.zip",
  "Temporal_PAHO": "resources/Temporal_extract_PAHO_V1_3.zip",
  "Temporal_SEARO": "resources/Temporal_extract_SEARO_V1_3.zip",
  "Temporal_WPRO": "resources/Temporal_extract_WPRO_V1_3.zip"
};

// Load and parse ZIP file CSV
async function loadAndParseZip(dataType, region) {
  const key = `${dataType}_${region}`;
  const url = zipFiles[key];
  if (!url) {
    alert(`No ZIP URL found for ${dataType} and ${region}`);
    return [];
  }

  try {
    const response = await fetch(url);
    if (!response.ok) {
      alert("Failed to fetch ZIP file.");
      return [];
    }

    const arrayBuffer = await response.arrayBuffer();
    const zip = await JSZip.loadAsync(arrayBuffer);
    const csvFileName = Object.keys(zip.files).find(name => name.endsWith(".csv"));
    if (!csvFileName) {
      alert("No CSV file found in ZIP.");
      return [];
    }

    const csvText = await zip.file(csvFileName).async("text");
    const parsed = Papa.parse(csvText, { header: true, skipEmptyLines: true });
    if (parsed.errors.length) {
      console.warn("CSV parse errors:", parsed.errors);
    }
    return parsed.data;
  } catch (err) {
    console.error("Error loading/parsing ZIP:", err);
    alert("Error loading or parsing ZIP file.");
    return [];
  }
}

// Populate country select options based on loaded data
function updateCountryOptions(data) {
  const countrySelect = document.getElementById("countrySelect");
  const uniqueCountries = [...new Set(data.map(row => row.adm_0_name).filter(Boolean))].sort();

  countrySelect.innerHTML = "";
  uniqueCountries.forEach(c => {
    const option = document.createElement("option");
    option.value = c;
    option.text = c;
    countrySelect.appendChild(option);
  });

  // Clear any previous selection
  countrySelect.value = null;
  // If using jQuery for change event:
  if (window.jQuery) {
    $(countrySelect).trigger("change");
  }
}

// DOMContentLoaded: set up event listeners
document.addEventListener("DOMContentLoaded", () => {
  const dataTypeSelect = document.getElementById("dataTypeSelect");
  const regionSelect = document.getElementById("regionSelect");
  const filterBtn = document.getElementById("filterBtn");
  const downloadBtn = document.getElementById("downloadBtn");

  dataTypeSelect.addEventListener("change", async () => {
    selectedDataType = dataTypeSelect.value;
    await tryLoadData();
  });

  regionSelect.addEventListener("change", async () => {
    selectedRegion = regionSelect.value;
    await tryLoadData();
  });

  async function tryLoadData() {
    // Only load if both selected
    if (!selectedDataType || !selectedRegion) {
      allData = [];
      updateCountryOptions([]);
      return;
    }
    allData = await loadAndParseZip(selectedDataType, selectedRegion);
    updateCountryOptions(allData);
  }

  filterBtn.addEventListener("click", () => {
    // Your filtering and preview logic here...
    // (You can expand this from your existing code)
  });

  downloadBtn.addEventListener("click", () => {
    // Your CSV download logic here...
  });
});
