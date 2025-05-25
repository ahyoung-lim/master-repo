pacman::p_load("dplyr", "lubridate", "tidyr", "tidyverse", "knitr", "stringi", "zoo", "data.table", "ggplot2", "sf", "rnaturalearth", "sf", "rnaturalearthdata", "distill", "downloadthis", "patchwork", "plotly", "showtext", "here", "htmlwidgets", "crosstalk")
showtext_auto()
font_add_google("Open Sans")

git_path <- "C:/Users/AhyoungLim/Dropbox/WORK/OpenDengue/master-repo-alim/master-repo/"
today <- gsub("-", "_", Sys.Date())
version <- "V1.3"
source(paste0(git_path, "scripts/Year_checker.R"))

prepare_heatmap_data <- function(version) {
  version_path <- gsub("_", ".", version)

  spatial_path <- paste0(git_path, "data/releases/", version_path, "/Spatial_extract_", version, ".csv")
  temporal_path <- paste0(git_path, "data/releases/", version_path, "/Temporal_extract_", version, ".csv")

  # Load and filter spatial data
  S_data <- read.csv(spatial_path) %>%
    filter(!adm_0_name %in% c("CANADA", "MONGOLIA", "NEW ZEALAND")) %>%
    Year_checker()

  # Load and filter temporal data
  T_data <- read.csv(temporal_path) %>%
    filter(!adm_0_name %in% c("CANADA", "MONGOLIA", "NEW ZEALAND")) %>%
    Year_checker()

  # Process temporal resolution
  temp_boolean <- T_data %>%
    mutate(T_res = ifelse(T_res == "Week", 2,
      ifelse(T_res == "Month", 1, 0)
    )) %>%
    group_by(adm_0_name, Year, T_res) %>%
    tally() %>%
    arrange(adm_0_name, Year, desc(T_res)) %>%
    group_by(adm_0_name, Year) %>%
    slice_head(n = 1) %>%
    select(-n) %>%
    ungroup() %>%
    complete(adm_0_name, Year) %>%
    mutate(
      T_res_nm = ifelse(T_res == 2, "Weekly",
        ifelse(T_res == 1, "Monthly", "Yearly")
      ),
      T_res_nm = factor(T_res_nm, levels = c("Weekly", "Monthly", "Yearly"))
    ) %>%
    select(-T_res)

  # Process spatial resolution
  spat_boolean <- S_data %>%
    mutate(S_res = ifelse(S_res == "Admin2", 2,
      ifelse(S_res == "Admin1", 1, 0)
    )) %>%
    group_by(adm_0_name, Year, S_res) %>%
    tally() %>%
    arrange(adm_0_name, Year, desc(S_res)) %>%
    group_by(adm_0_name, Year) %>%
    slice_head(n = 1) %>%
    select(-n) %>%
    ungroup() %>%
    complete(adm_0_name, Year) %>%
    mutate(
      S_res_nm = ifelse(S_res == 2, "Admin2",
        ifelse(S_res == 1, "Admin1", "Admin0")
      ),
      S_res_nm = factor(S_res_nm, levels = c("Admin2", "Admin1", "Admin0"))
    ) %>%
    select(-S_res)

  # Combine and clean
  dt_heatmap <- merge(temp_boolean, spat_boolean, by = c("adm_0_name", "Year"), all = TRUE) %>%
    mutate(
      adm_0_name = tools::toTitleCase(tolower(adm_0_name)),
      adm_0_name = ifelse(adm_0_name == "Virgin Islands (Uk)", "Virgin Islands (UK)",
        ifelse(adm_0_name == "Virgin Islands (Us)", "Virgin Islands (US)", adm_0_name)
      )
    )

  return(dt_heatmap)
}

Versions <- c("V1_0", "V1_1", "V1_2_2", "V1_3")

dt_list <- lapply(Versions, function(version) {
  prepare_heatmap_data(version) %>%
    mutate(version = version)
})

data <- rbindlist(dt_list)


# Define WHO regions with their respective countries
# Define WHO regions
who_regions <- list(
  PAHO = c(
    "American Samoa", "Anguilla", "Antigua and Barbuda", "Argentina", "Aruba", "Bahamas",
    "Barbados", "Belize", "Bermuda", "Bolivia", "Bonaire, Saint Eustatius and Saba", "Brazil",
    "Cayman Islands", "Chile", "Colombia", "Costa Rica", "Cuba", "Curacao", "Dominica",
    "Dominican Republic", "Ecuador", "El Salvador", "French Guiana", "Grenada", "Guadeloupe",
    "Guatemala", "Guyana", "Haiti", "Honduras", "Jamaica", "Martinique", "Mexico", "Montserrat",
    "Nicaragua", "Panama", "Paraguay", "Peru", "Puerto Rico", "Saint Barthelemy",
    "Saint Kitts and Nevis", "Saint Lucia", "Saint Martin", "Saint Vincent and the Grenadines",
    "Sint Maarten", "Suriname", "Trinidad and Tobago", "Turks and Caicos Islands",
    "United States of America", "Uruguay", "Venezuela", "Virgin Islands (UK)", "Virgin Islands (US)"
  ),
  WPRO = c(
    "Australia", "Brunei Darussalam", "Cambodia", "China", "Cook Islands", "Fiji",
    "French Polynesia", "Guam", "Hong Kong", "Japan", "Kiribati", "Lao People's Democratic Republic",
    "Macau", "Malaysia", "Marshall Islands", "Micronesia (Federated States of)", "Nauru",
    "New Caledonia", "Niue", "Northern Mariana Islands", "Palau",
    "Papua New Guinea", "Philippines", "Pitcairn", "Republic of Korea", "Samoa", "Singapore",
    "Solomon Islands", "Taiwan", "Tokelau", "Tonga", "Tuvalu",
    "Vanuatu", "Viet Nam", "Wallis and Futuna"
  ),
  SEARO = c(
    "Bangladesh", "Bhutan", "India", "Indonesia", "Maldives", "Myanmar",
    "Nepal", "Sri Lanka", "Thailand", "Timor-Leste"
  ),
  EMRO = c(
    "Afghanistan", "Pakistan", "Saudi Arabia", "Oman", "Yemen", "Sudan"
  ),
  AFRO = c(
    "Angola", "Benin", "Burkina Faso", "Cabo Verde", "Cameroon",
    "Central African Republic", "Chad", "Cote D'ivoire", "Eritrea", "Ethiopia",
    "Ghana", "Guinea", "Kenya", "Mali", "Mauritania", "Mauritius", "Mayotte",
    "Niger", "Reunion", "Sao Tome and Principe", "Senegal", "Seychelles",
    "Togo", "United Republic of Tanzania"
  ),
  EURO = c(
    "France", "Italy", "Spain"
  )
)

# Create WHO region lookup
who_region_lookup <- do.call(rbind, lapply(names(who_regions), function(region) {
  data.frame(
    adm_0_name = who_regions[[region]],
    region = region,
    stringsAsFactors = FALSE
  )
}))

setdiff(unique(data$adm_0_name), unique(who_region_lookup$adm_0_name))

# Merge with main dataset
data <- data %>%
  left_join(who_region_lookup, by = "adm_0_name")

who_region_lookup %>%
  count(adm_0_name) %>%
  filter(n > 1)


write.csv(data, paste0(git_path, "heatmap_js/heatmap.csv"), row.names = F)

# Sample input: combine all data into one data.table
# Assuming 'data' is your full dataset with multiple versions
# Columns: adm_0_name, Year, T_res_nm, S_res_nm, region, subregion, version

# Convert to data.table for efficient processing
data <- as.data.table(data)

# Split data by version
versions <- unique(data$version)
if (length(versions) < 2) stop("At least two versions needed for comparison.")

# Sort versions to get 'previous' and 'current'
versions <- sort(versions)
prev_data <- data[version == versions[length(versions) - 1]]
curr_data <- data[version == versions[length(versions)]]

# 1. New countries in current version
new_countries <- setdiff(unique(curr_data$adm_0_name), unique(prev_data$adm_0_name))
cat("1. New countries added:", length(new_countries), "\n")

# 2. New country-year tiles for existing countries
common_countries <- intersect(unique(curr_data$adm_0_name), unique(prev_data$adm_0_name))

prev_tiles <- prev_data[adm_0_name %in% common_countries, unique(paste(adm_0_name, Year, sep = "_"))]
curr_tiles <- curr_data[adm_0_name %in% common_countries, unique(paste(adm_0_name, Year, sep = "_"))]

new_tiles <- setdiff(curr_tiles, prev_tiles)
cat("2. New country-year tiles for existing countries:", length(new_tiles), "\n")

# 3. Improved resolution for existing country-years
# Define resolution levels
temporal_levels <- c("Yearly" = 1, "Monthly" = 2, "Weekly" = 3)
spatial_levels <- c("Admin2" = 2, "Admin1" = 1) # Admin2 finer than Admin1

# Add resolution levels
prev_data[, t_level := temporal_levels[T_res_nm]]
prev_data[, s_level := spatial_levels[S_res_nm]]
curr_data[, t_level := temporal_levels[T_res_nm]]
curr_data[, s_level := spatial_levels[S_res_nm]]

# Merge previous and current data by country-year
merged <- merge(
  prev_data, curr_data,
  by = c("adm_0_name", "Year"),
  suffixes = c("_prev", "_curr")
)

# Identify improved resolution
merged[, improved := (t_level_curr > t_level_prev) | (s_level_curr > s_level_prev)]
n_improved <- sum(merged$improved, na.rm = TRUE)
cat("3. Improved resolution for existing country-years:", n_improved, "\n")
