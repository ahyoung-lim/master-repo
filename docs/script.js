console.log("script.js loaded");

const baseUrl = "https://ahyoung-lim.github.io/master-repo/resources/";

function getZipFileName(dataType, region) {
  return `${dataType}_extract_${region}_V1_3.zip`;
}

let selectedDataType = null;
let selectedRegion = null;
let countries = [];
let dateRange = [null, null];
let allData = [];
let filteredPreview = [];

function convertToCSV(data) {
  return Papa.unparse(data);
}

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

async function loadAndParseZip(dataType, region) {
  if (!dataType || !region) {
    console.warn("Data type or region not selected.");
    return [];
  }
  const zipFileName = getZipFileName(dataType, region);
  const url = baseUrl + zipFileName;
  console.log(`Fetching ZIP: ${url}`);

  try {
    const response = await fetch(url);
    if (!response.ok) {
      alert(`Failed to fetch ZIP file: ${zipFileName}`);
      console.error(`Fetch error: ${response.status} ${response.statusText}`);
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
    console.log(`Parsed CSV from ZIP: ${csvFileName}`);

    const parsed = Papa.parse(csvText, { header: true, skipEmptyLines: true });
    if (parsed.errors.length) {
      console.warn("CSV parse errors:", parsed.errors);
    }

    console.log(`Loaded ${parsed.data.length} rows.`);
    return parsed.data;
  } catch (error) {
    console.error("Error loading or parsing ZIP:", error);
    alert("Error loading or parsing ZIP file.");
    return [];
  }
}

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

    return (rowStart <= end) && (rowEnd >= start);
  });
}

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

  countrySelect.selectedIndex = -1; // clear selection properly
  $(countrySelect).trigger("change");
}

function computeDateRange(data, countries) {
  const filtered = data.filter(row => countries.includes(row.adm_0_name));
  if (!filtered.length) return [null, null];

  const startDates = filtered.map(r => new Date(r.calendar_start_date));
  const endDates = filtered.map(r => new Date(r.calendar_end_date));

  const minStart = new Date(Math.min(...startDates));
  const maxEnd = new Date(Math.max(...endDates));

  return [minStart.toISOString().slice(0,10), maxEnd.toISOString().slice(0,10)];
}

document.addEventListener("DOMContentLoaded", () => {
  console.log("DOM fully loaded and parsed");

  const dataTypeSelect = document.getElementById("dataTypeSelect");
  const regionSelect = document.getElementById("regionSelect");
  const countrySelect = document.getElementById("countrySelect");
  const startDateInput = document.getElementById("startDate");
  const endDateInput = document.getElementById("endDate");
  const filterBtn = document.getElementById("filterBtn");
  const downloadBtn = document.getElementById("downloadBtn");

  async function loadDataAndUpdate() {
    selectedDataType = dataTypeSelect.value;
    selectedRegion = regionSelect.value;
    allData = [];
    filteredPreview = [];
    renderPreviewTable([]);
    downloadBtn.href = "#";
    downloadBtn.textContent = "Download filtered CSV";

    if (!selectedDataType || !selectedRegion) {
      console.log("Waiting for both Data Type and Region to be selected.");
      return;
    }

    downloadBtn.textContent = "Loading data... please wait";

    allData = await loadAndParseZip(selectedDataType, selectedRegion);
    if (allData.length === 0) {
      downloadBtn.textContent = "Download filtered CSV";
      alert("No data loaded.");
      return;
    }

    updateCountryOptions(allData);

    startDateInput.value = "";
    endDateInput.value = "";
    startDateInput.min = "";
    startDateInput.max = "";
    endDateInput.min = "";
    endDateInput.max = "";

    downloadBtn.textContent = "Download filtered CSV";
    console.log("Data loaded and country options updated.");
  }

  dataTypeSelect.addEventListener("change", loadDataAndUpdate);
  regionSelect.addEventListener("change", loadDataAndUpdate);

  $(countrySelect).on("change", () => {
    countries = Array.from(countrySelect.selectedOptions).map(o => o.value);
    if (countries.length && allData.length) {
      const [minDate, maxDate] = computeDateRange(allData, countries);
      if (minDate && maxDate) {
        startDateInput.min = minDate;
        startDateInput.max = maxDate;
        endDateInput.min = minDate;
        endDateInput.max = maxDate;

        startDateInput.value = minDate;
        endDateInput.value = maxDate;

        dateRange = [minDate, maxDate];
      } else {
        alert("No overlapping date ranges for selected countries.");
        startDateInput.value = "";
        endDateInput.value = "";
        startDateInput.min = "";
        startDateInput.max = "";
        endDateInput.min = "";
        endDateInput.max = "";
        dateRange = [null, null];
      }
    } else {
      startDateInput.value = "";
      endDateInput.value = "";
      startDateInput.min = "";
      startDateInput.max = "";
      endDateInput.min = "";
      endDateInput.max = "";
      dateRange = [null, null];
    }
    renderPreviewTable([]);
  });

  startDateInput.addEventListener("change", () => {
    dateRange[0] = startDateInput.value;
  });

  endDateInput.addEventListener("change", () => {
    dateRange[1] = endDateInput.value;
  });

  filterBtn.addEventListener("click", () => {
    if (!selectedDataType) {
      alert("Please select a data type.");
      return;
    }
    if (!selectedRegion) {
      alert("Please select a region.");
      return;
    }
    if (!countries.length) {
      alert("Please select at least one country.");
      return;
    }
    if (!dateRange[0] || !dateRange[1]) {
      alert("Please select valid start and end dates.");
      return;
    }
    if (dateRange[0] > dateRange[1]) {
      alert("Start date must be before or equal to end date.");
      return;
    }
    filteredPreview = filterData(allData, countries, dateRange[0], dateRange[1]);
    console.log("Filtered rows:", filteredPreview.length);
    if (!filteredPreview.length) {
      alert("No data matches your filter.");
    }
    renderPreviewTable(filteredPreview);
  });

  downloadBtn.addEventListener("click", () => {
    if (!filteredPreview.length) {
      alert("No filtered data to download. Please preview first.");
      return;
    }
    const csv = convertToCSV(filteredPreview);
    const fileName = `filtered_${selectedDataType}_${selectedRegion}.csv`;
    downloadCSV(csv, fileName);
  });
});
