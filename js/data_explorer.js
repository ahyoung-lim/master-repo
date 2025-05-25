document.addEventListener('DOMContentLoaded', async function() {
  // --- Data Variables ---
  let nationalData, temporalData, spatialData, nationalNames;
  let currentDataType = 'National_extract_V1_2_2.csv'; // Default data type
  let currentData = []; // Data currently loaded and filtered

  // --- UI Elements ---
  const dataTypeSelect = document.getElementById('dataType');
  const countryInput = document.getElementById('countrySelect'); // Corrected ID
  const startDateInput = document.getElementById('startDate');
  const endDateInput = document.getElementById('endDate');
  const processBtn = document.getElementById('processBtn');
  const downloadDataBtn = document.getElementById('downloadBtn'); // Corrected ID
  const loadingOverlay = document.querySelector('.loading-overlay');
  const tabLinks = document.querySelectorAll('.nav-tabs .nav-link');
  const tabPanes = document.querySelectorAll('.tab-content .tab-pane');

  let dataTableInstance; // To hold the DataTables.js instance

  // --- Helper Functions ---

  // Show/Hide Loading Indicator
  function showLoading() { loadingOverlay.style.display = 'flex'; }
  function hideLoading() { loadingOverlay.style.display = 'none'; }

  // Parse CSV data
  async function parseCSV(url) {
    const response = await fetch(url);
    const text = await response.text();
    const lines = text.split('\n').filter(line => line.trim() !== '');
    const headers = lines[0].split(',').map(h => h.trim());
    return lines.slice(1).map(line => {
      const values = line.split(',').map(v => v.trim());
      let obj = {};
      headers.forEach((header, i) => {
        if (header.includes('date') && values[i]) {
          const date = new Date(values[i]);
          obj[header] = isNaN(date.getTime()) ? values[i] : date;
        } else if (!isNaN(Number(values[i])) && values[i] !== '') {
          obj[header] = Number(values[i]);
        } else {
          obj[header] = values[i];
        }
      });
      return obj;
    });
  }

  // Aggregate data for Plotly
  function aggregateData(data) {
    const aggregated = {};
    data.forEach(row => {
      const key = `${row.calendar_start_date}-${row.calendar_end_date}-${row.S_res}-${row.T_res}`;
      if (!aggregated[key]) {
        aggregated[key] = {
          calendar_start_date: row.calendar_start_date,
          calendar_end_date: row.calendar_end_date,
          S_res: row.S_res,
          T_res: row.T_res,
          dengue_total: 0
        };
      }
      aggregated[key].dengue_total += row.dengue_total || 0;
    });
    return Object.values(aggregated);
  }

  // Render Plotly Plot
  function renderPlotly(data) {
    const plotDiv = document.getElementById('plotDiv');
    const traces = [];
    const layout = {
      title: 'Dengue Total Over Time',
      xaxis: { title: 'Date' },
      yaxis: { title: 'Dengue Total' },
      hovermode: 'closest'
    };

    const groupedByTRes = data.reduce((acc, item) => {
      acc[item.T_res] = acc[item.T_res] || [];
      acc[item.T_res].push(item);
      return acc;
    }, {});

    for (const T_res in groupedByTRes) {
      const groupData = groupedByTRes[T_res].sort((a, b) => {
        const dateA = a.calendar_start_date instanceof Date ? a.calendar_start_date.getTime() : new Date(a.calendar_start_date).getTime();
        const dateB = b.calendar_start_date instanceof Date ? b.calendar_start_date.getTime() : new Date(b.calendar_start_date).getTime();
        return dateA - dateB;
      });
      traces.push({
        x: groupData.map(d => d.calendar_start_date),
        y: groupData.map(d => d.dengue_total),
        mode: 'lines+markers',
        name: T_res,
        type: 'scatter'
      });
    }

    Plotly.newPlot(plotDiv, traces, layout);
  }

  // --- Event Listeners ---

  // Load all data on page load
  async function loadAllData() {
    showLoading();
    try {
      nationalNames = (await parseCSV('data/national_names.csv')).map(row => row.x);
      nationalData = await parseCSV('data/National_extract_V1_2_2.csv');
      temporalData = await parseCSV('data/Temporal_extract_V1_2_2.csv');
      spatialData = await parseCSV('data/JS_data/Spatial_extract_V1_2_2.csv'); // Assuming this path is correct if it's different

      // Populate country picker
      countryInput.innerHTML = ''; // Clear existing options
      nationalNames.forEach(name => {
        const option = document.createElement('option');
        option.value = name;
        option.textContent = name;
        countryInput.appendChild(option);
      });
      // Select 'AMERICAN SAMOA' by default
      if (countryInput.options.length > 0) {
        countryInput.value = 'AMERICAN SAMOA';
      }

      // Set initial date range from nationalData
      const allDates = nationalData.map(d => d.calendar_start_date).filter(d => d instanceof Date);
      if (allDates.length > 0) {
        const minDate = new Date(Math.min(...allDates));
        const maxDate = new Date(Math.max(...allDates));
        startDateInput.value = minDate.toISOString().split('T')[0];
        endDateInput.value = maxDate.toISOString().split('T')[0];
      } else {
        startDateInput.value = "1955-01-01";
        endDateInput.value = "2010-12-31";
      }

    } catch (error) {
      console.error("Failed to load data:", error);
      alert("Error loading data. Please check console for details.");
    } finally {
      hideLoading();
    }
  }

  // Handle data type selection change
  dataTypeSelect.addEventListener('change', function() {
    currentDataType = this.value;
    downloadDataBtn.disabled = true;
  });

  // Process button click (exposed globally for onclick attribute)
  window.processData = async function() {
    showLoading();
    downloadDataBtn.disabled = true;

    try {
      let selectedData;
      switch (currentDataType) {
        case 'National_extract_V1_2_2.csv': selectedData = nationalData; break;
        case 'Temporal_extract_V1_2_2.csv': selectedData = temporalData; break;
        case 'Spatial_extract_V1_2_2.csv': selectedData = spatialData; break;
        default: selectedData = [];
      }

      const selectedCountries = Array.from(countryInput.selectedOptions).map(option => option.value);
      const startDate = new Date(startDateInput.value);
      const endDate = new Date(endDateInput.value);

      if (isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
        alert("Please select a valid date range.");
        hideLoading();
        return;
      }

      currentData = selectedData.filter(row => {
        const rowStartDate = row.calendar_start_date instanceof Date ? row.calendar_start_date : new Date(row.calendar_start_date);
        const rowEndDate = row.calendar_end_date instanceof Date ? row.calendar_end_date : new Date(row.calendar_end_date);

        const countryMatch = selectedCountries.includes(row.adm_0_name);
        const dateMatch = rowStartDate.getTime() >= startDate.getTime() && rowEndDate.getTime() <= endDate.getTime();
        return countryMatch && dateMatch;
      });

      // Render Table using DataTables.js
      if (dataTableInstance) {
        dataTableInstance.destroy(); // Destroy existing instance
        $('#dataTable').empty(); // Clear table content
      }

      if (currentData.length > 0) {
        const columns = Object.keys(currentData[0]).map(key => ({ title: key.replace(/_/g, ' '), data: key })); // Prettify headers
        dataTableInstance = $('#dataTable').DataTable({
          data: currentData,
          columns: columns,
          scrollX: true,
          scrollY: "50vh",
          scrollCollapse: true,
          paging: true,
          searching: true,
          info: true,
          destroy: true
        });
      } else {
        $('#dataTable').empty().append('<thead><tr><th>No Data Available</th></tr></thead><tbody><tr><td></td></tr></tbody>');
      }

      // Render Plot
      const aggregatedPlotData = aggregateData(currentData);
      renderPlotly(aggregatedPlotData);

      downloadDataBtn.disabled = false;
    } catch (error) {
      console.error("Error processing data:", error);
      alert("An error occurred during processing. Check console for details.");
    } finally {
      hideLoading();
    }
  }; // End processData function

  // Download data button click (exposed globally for onclick attribute)
  window.downloadCSV = function() {
    if (currentData.length === 0) {
      alert("No data to download.");
      return;
    }

    const headers = Object.keys(currentData[0]);
    const csvRows = [];
    csvRows.push(headers.join(','));
    currentData.forEach(row => {
      const values = headers.map(header => {
        const value = row[header];
        if (typeof value === 'string' && (value.includes(',') || value.includes('"'))) {
          return `"${value.replace(/"/g, '""')}"`;
        }
        return value instanceof Date ? value.toISOString().split('T')[0] : value; // Format dates for CSV
      });
      csvRows.push(values.join(','));
    });
    const csvString = csvRows.join('\n');

    const blob = new Blob([csvString], { type: 'text/csv;charset=utf-8;' });
    const filename = `${currentDataType.replace('.csv', '')}_filtered.csv`;
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  }; // End downloadCSV function

  // Tab switching logic (using Bootstrap's JS for tabs)
  const tabButtons = document.querySelectorAll('#myTab button[data-bs-toggle="tab"]');
  tabButtons.forEach(button => {
    button.addEventListener('shown.bs.tab', function (event) {
      const targetTabId = event.target.dataset.bsTarget.replace('#', ''); // e.g., 'table' or 'plot'

      // Special handling for Plotly to redraw when its tab becomes active
      if (targetTabId === 'plot' && currentData.length > 0) {
        renderPlotly(aggregateData(currentData));
      }
      // For DataTables, ensure it redraws if its container was hidden
      if (targetTabId === 'table' && dataTableInstance) {
        dataTableInstance.columns.adjust().draw();
      }
    });
  });

  // Initial data load on page load
  loadAllData();
});
