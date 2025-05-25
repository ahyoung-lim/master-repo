// Define the base URL for GitHub Pages (adjust if repo name or username is different)
const baseUrl = "https://ahyoung-lim.github.io/master-repo/data/";

// Map of available zip files
const zipFiles = {
  "National_AFRO": `${baseUrl}National_extract_AFRO_V1_3.zip`,
  "National_WPRO": `${baseUrl}National_extract_WPRO_V1_3.zip`,
  "National_SEARO": `${baseUrl}National_extract_SEARO_V1_3.zip`,
  "National_EMRO": `${baseUrl}National_extract_EMRO_V1_3.zip`,
  "National_EURO": `${baseUrl}National_extract_EURO_V1_3.zip`,
  "National_PAHO": `${baseUrl}National_extract_PAHO_V1_3.zip`,
  "Spatial_AFRO": `${baseUrl}Spatial_extract_AFRO_V1_3.zip`,
  "Spatial_WPRO": `${baseUrl}Spatial_extract_WPRO_V1_3.zip`,
  "Spatial_SEARO": `${baseUrl}Spatial_extract_SEARO_V1_3.zip`,
  "Spatial_EMRO": `${baseUrl}Spatial_extract_EMRO_V1_3.zip`,
  "Spatial_EURO": `${baseUrl}Spatial_extract_EURO_V1_3.zip`,
  "Spatial_PAHO": `${baseUrl}Spatial_extract_PAHO_V1_3.zip`,
  "Temporal_AFRO": `${baseUrl}Temporal_extract_AFRO_V1_3.zip`,
  "Temporal_WPRO": `${baseUrl}Temporal_extract_WPRO_V1_3.zip`,
  "Temporal_SEARO": `${baseUrl}Temporal_extract_SEARO_V1_3.zip`,
  "Temporal_EMRO": `${baseUrl}Temporal_extract_EMRO_V1_3.zip`,
  "Temporal_EURO": `${baseUrl}Temporal_extract_EURO_V1_3.zip`,
  "Temporal_PAHO": `${baseUrl}Temporal_extract_PAHO_V1_3.zip`,
};

// Listen for clicks on the "Preview" button
document.getElementById("filterBtn").addEventListener("click", () => {
  const dataType = document.getElementById("dataTypeSelect").value;
  const region = document.getElementById("regionSelect").value;

  const zipKey = `${dataType}_${region}`;
  const zipUrl = zipFiles[zipKey];

  if (!zipUrl) {
    alert("Invalid selection or file not available.");
    return;
  }

  fetch(zipUrl)
    .then(response => {
      if (!response.ok) throw new Error("Network response was not ok");
      return response.blob();
    })
    .then(JSZip.loadAsync)
    .then(zip => {
      const csvFileName = Object.keys(zip.files).find(name => name.endsWith(".csv"));
      return zip.files[csvFileName].async("string");
    })
    .then(csvText => {
      const parsed = Papa.parse(csvText, { header: true });
      renderTable(parsed.data);
    })
    .catch(error => {
      console.error("Error loading ZIP:", error);
      alert("Failed to load or parse the data.");
    });
});

// Render table using DataTables
function renderTable(data) {
  if ($.fn.DataTable.isDataTable("#previewTable")) {
    $('#previewTable').DataTable().clear().destroy();
    $('#previewTable').empty(); // Clear existing table header
  }

  if (!data.length) return;

  const columns = Object.keys(data[0]).map(key => ({ title: key, data: key }));

  $('#previewTable').DataTable({
    data,
    columns
  });
}

// Handle CSV download
document.getElementById("downloadBtn").addEventListener("click", () => {
  const dataType = document.getElementById("dataTypeSelect").value;
  const region = document.getElementById("regionSelect").value;

  const zipKey = `${dataType}_${region}`;
  const zipUrl = zipFiles[zipKey];

  if (!zipUrl) {
    alert("Invalid selection or file not available.");
    return;
  }

  fetch(zipUrl)
    .then(response => {
      if (!response.ok) throw new Error("Network response was not ok");
      return response.blob();
    })
    .then(blob => {
      const link = document.createElement("a");
      link.href = URL.createObjectURL(blob);
      link.download = zipUrl.split('/').pop();
      link.click();
    })
    .catch(error => {
      console.error("Download error:", error);
      alert("Failed to download the file.");
    });
});
