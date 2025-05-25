// Base URL for your zip files on GitHub Pages (adjust if needed)
const baseUrl = "https://ahyoung-lim.github.io/master-repo/resources/";

// Data type to zip file name pattern function
function getZipFileName(dataType, region) {
  return `${dataType}_extract_${region}_V1_3.zip`;
}

let selectedDataType = null;
let selectedRegion = null;
let countries = [];
let dateRange = [null, null];
let allData = [];  // all parsed CSV rows as JS objects
let filteredPreview = [];

// Utility: convert filtered data array to CSV string
function convertToCSV(data) {
  return Papa.unparse(data);
}

// Utility: download CSV file from string content
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

    console.log(`Loaded ${parsed.data.length} rows.`);
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

  if (!filtered.length) return [null, null];

  const startDates = filtered.map(r => new Date(r.calendar_start_date));
  const endDates = filtered.map(r => new Date(r.calendar_end_date));

  const maxStart = new Date(Math.max(...startDates));
  const minEnd = new Date(Math.min(...endDates));

  if (maxStart > minEnd) return [null, null];

  return [maxStart.toISOString().slice(0,10), minEnd.toISOString().slice(0,10)];
}

// Helper: parse date string to Date object safely
function parseDate(dateStr) {
  return dateStr ? new Date(dateStr) : null;
}

// ISO week number helper function
function getISOWeekNumber(date) {
  const tmpDate = new Date(date.getTime());
  tmpDate.setHours(0, 0, 0, 0);
  tmpDate.setDate(tmpDate.getDate() + 3 - ((tmpDate.getDay() + 6) % 7));
  const week1 = new Date(tmpDate.getFullYear(), 0, 4);
  return 1 + Math.round(((tmpDate.getTime() - week1.getTime()) / 86400000 - 3 + ((week1.getDay() + 6) % 7)) / 7);
}

// Aggregate data counts by period (week/month/year)
function aggregateByPeriod(data, period) {
  const counts = {};

  data.forEach(row => {
    const date = parseDate(row.calendar_start_date);
    if (!date) return;

    let key;
    if (period === 'week') {
      const weekNum = getISOWeekNumber(date);
      key = `${date.getFullYear()}-W${weekNum}`;
    } else if (period === 'month') {
      key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
    } else if (period === 'year') {
      key = `${date.getFullYear()}`;
    }
    counts[key] = (counts[key] || 0) + 1;
  });

  const sortedKeys = Object.keys(counts).sort();

  return {
    x: sortedKeys,
    y: sortedKeys.map(k => counts[k])
  };
}

// Plot function: render plot by period id and data
function renderPlot(periodId, periodName, data) {
  const trace = {
    x: data.x,
    y: data.y,
    type: 'scatter',
    mode: 'lines+markers',
    marker: {color: 'blue'}
  };
  const layout = {
    title: `${periodName} Data`,
    xaxis: { title: periodName },
    yaxis: { title: 'Count' },
    margin: { t: 40, b: 50 }
  };
  Plotly.newPlot(periodId, [trace], layout, {responsive: true});
}

// Update all three plots
function updatePlots(filteredData) {
  if (!filteredData.length) {
    ['plot-weekly', 'plot-monthly', 'plot-yearly'].forEach(id => {
      document.getElementById(id).innerHTML = "<p>No data to display</p>";
    });
    return;
  }

  renderPlot('plot-weekly', 'Weekly', aggregateByPeriod(filteredData, 'week'));
  renderPlot('plot-monthly', 'Monthly', aggregateByPeriod(filteredData, 'month'));
  renderPlot('plot-yearly', 'Yearly', aggregateByPeriod(filteredData, 'year'));
}

// DOMContentLoaded handler
document.addEventListener("DOMContentLoaded", () => {
  const dataTypeSelect = document.getElementById("dataTypeSelect");
  const regionSelect = document.getElementById("regionSelect");
  const countrySelect = document.getElementById("countrySelect");
  const startDateInput = document.getElementById("startDate");
  const endDateInput = document.getElementById("endDate");
  const filterBtn = document.getElementById("filterBtn");
  const downloadBtn = document.getElementById("downloadBtn");

  // When data type or region changes: load ZIP, parse, update countries
  async function loadDataAndUpdate() {
    selectedDataType = dataTypeSelect.value;
    selectedRegion = regionSelect.value;
    allData = [];

    if (selectedDataType && selectedRegion) {
      allData = await loadAndParseZip(selectedDataType, selectedRegion);
      updateCountryOptions(allData);

      // Clear date inputs and plots
      startDateInput.value = "";
      endDateInput.value = "";
      ['plot-weekly', 'plot-monthly', 'plot-yearly'].forEach(id => {
        document.getElementById(id).innerHTML = "<p>No data loaded yet</p>";
      });

      // Clear preview table
      if ($.fn.dataTable.isDataTable('#previewTable')) {
        $('#previewTable').DataTable().clear().destroy();
      }
    }
  }

  dataTypeSelect.addEventListener("change", loadDataAndUpdate);
  regionSelect.addEventListener("change", loadDataAndUpdate);

  // When countries selected: update date range inputs
  $(countrySelect).on("change", () => {
    countries = Array.from(countrySelect.selectedOptions).map(opt => opt.value);

    if (countries.length) {
      const [minDate, maxDate] = computeDateRange(allData, countries);
      if (minDate && maxDate) {
        startDateInput.min = minDate;
        startDateInput.max = maxDate;
        endDateInput.min = minDate;
        endDateInput.max = maxDate;

        // Reset values to min and max if out of bounds
        if (!startDateInput.value || startDateInput.value < minDate || startDateInput.value > maxDate) {
          startDateInput.value = minDate;
        }
        if (!endDateInput.value || endDateInput.value < minDate || endDateInput.value > maxDate) {
          endDateInput.value = maxDate;
        }
      } else {
        startDateInput.value = "";
        endDateInput.value = "";
      }
    } else {
      startDateInput.value = "";
      endDateInput.value = "";
    }
  });

  // Filter button click
  filterBtn.addEventListener("click", () => {
    if (!countries.length) {
      alert("Please select at least one country.");
      return;
    }
    if (!startDateInput.value || !endDateInput.value) {
      alert("Please select a valid date range.");
      return;
    }
    if (startDateInput.value > endDateInput.value) {
      alert("Start date must be before or equal to end date.");
      return;
    }

    dateRange = [startDateInput.value, endDateInput.value];

    filteredPreview = filterData(allData, countries, dateRange[0], dateRange[1]);

    if (!filteredPreview.length) {
      alert("No data matches your filter.");
    }

    renderPreviewTable(filteredPreview);
    updatePlots(filteredPreview);
  });

  // Download button click
  downloadBtn.addEventListener("click", () => {
    if (!filteredPreview.length) {
      alert("No filtered data to download.");
      return;
    }
    // Create custom filename
    const countryStr = countries.join("-");
    const fileName = `data_${selectedDataType}_${selectedRegion}_${countryStr}_${dateRange[0]}_to_${dateRange[1]}.csv`;

    const csvString = convertToCSV(filteredPreview);
    downloadCSV(csvString, fileName);
  });
});
