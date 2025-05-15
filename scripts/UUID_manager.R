rm(list = ls())
library(readxl)
library(countrycode)
library(dplyr)
library(data.table)
library(tidyr)
library(lubridate)
setwd("C:/Users/AhyoungLim/Dropbox/WORK/OpenDengue/11_DENData/")
git_path <- "C:/Users/AhyoungLim/Dropbox/WORK/OpenDengue/master-repo-alim/master-repo/"
OD_path <- "C:/Users/AhyoungLim/Dropbox/WORK/OpenDengue"
today <- gsub("-", "_", Sys.Date())
release <- TRUE

# fmt: skip
# load in filing DBs for V1.0 and V1.1
f <- read_excel(paste0(git_path, "./data/archive/filingDB_V1.0.xlsx"), sheet = 1)
f1 <- read_excel(
  paste0(git_path, "./data/archive/filingDB_V1.1.xlsx"),
  sheet = 1
) %>%
  mutate(UUID_old = NA, original_filename = NA, NOTES = NA)

names(f)
names(f1)

f$version <- "V1.0"
f1$version <- "V1.1"

# fmt: skip
m <- rbind(
  f[, c(
    "version", "original_filename", "standard_filename",
    "source_cat", "country", "year",
    "serial_cat", "serial_cat_num", "serial_id",
    "NOTES", "UUID_old", "metadata_description",
    "metadata_url", "metadata_steps"
  )],
  f1[, c(
    "version", "original_filename", "standard_filename",
    "source_cat", "country", "year",
    "serial_cat", "serial_cat_num", "serial_id",
    "NOTES", "UUID_old", "metadata_description",
    "metadata_url", "metadata_steps"
  )]
)

# add in new filing DB here =====
f3 <- read.csv(paste0(git_path, "./open_dengue_1.3/filingDB_V1.3.csv")) %>%
  select(-(case_definition_original:case_definition_standardised)) %>%
  rename(year = period) %>%
  filter(
    !metadata_url ==
      "https://www3.paho.org/data/index.php/es/temas/indicadores-dengue/dengue-regional/506-dengue-reg-ano-es.html"
  )
# filter(grepl("Mexico|PAHO|paho", standard_filename)) # remove Nepal, ECDC, Peru data for now

m <- rbind(m, f3)
rm(f, f1, f3)

# update serial id for Mexico
m$serial_id[grepl("Mexico_2007", m$standard_filename)] <- as.numeric(sub(
  ".*EW(\\d+).*",
  "\\1",
  m$standard_filename[grepl("Mexico_2007", m$standard_filename)]
))

# !!!! check if any duplicated file records
m %>%
  group_by(standard_filename, metadata_description, metadata_url) %>%
  tally() %>%
  arrange(desc(n))

m %>%
  group_by(standard_filename) %>%
  tally() %>%
  arrange(desc(n))

# remove duplicates
m <- m %>%
  filter(!(grepl("20190103|20201217", standard_filename) & is.na(UUID_old)))

# check filename extensions
summary(grepl("\\.[^.]+$", m$standard_filename)) # should be all TRUE
m$standard_filename[grepl("\\.[^.]+$", m$standard_filename) == FALSE]

#####################################################################

# V1.0 ==============================================================

#####################################################################
# merge, process, and standardise datasets for V1.0

# load in MOH_PAHO_subnational data (with error correction)
source(
  "C:/Users/AhyoungLim/Dropbox/WORK/OpenDengue/OD_error_checking/MOH_PAHO_SUB_correction.R"
)

# TYCHO
db3 <- read.csv("01_Dengue_data/open_dengue_1.0/TYCHO_transformed.csv")

db1 <- db1 %>%
  select(adm_0_name:dengue_total, source_cat, UUID, source_id) %>%
  mutate(
    source_cat = ifelse(source_cat %in% c("paho"), "PAHO_PLISA", "PAHO_MOH")
  ) %>%
  filter(!UUID %in% c("9e92834d-c739-11ed-aa9a-1e00d12e8869")) %>% # remove duplicates (PANAMA 2018-2022 EW09 )
  mutate(raw_data = "V1_0_MOH_PAHO_SUB_transformed.csv")

db3 <- db3 %>%
  select(adm_0_name:dengue_total, source_cat, UUID) %>%
  mutate(source_id = "tycho_dengue_allcountries.csv") %>%
  mutate(raw_data = "V1_0_TYCHO_transformed.csv")

names(db1)
names(db3)

db1 <- rbind(db1, db3)

names(db1)[11] <- "UUID_old"
names(db1)[12] <- "standard_filename"
summary(is.na(db1$UUID_old))
summary(is.na(db1$standard_filename))

db1 <- merge(
  db1,
  m[, c("UUID_old", "standard_filename")],
  by = c("UUID_old", "standard_filename"),
  all.x = T
) %>%
  mutate(original_filename = NA) %>%
  select(
    standard_filename,
    adm_0_name:source_cat,
    UUID_old,
    original_filename,
    raw_data
  )

summary(is.na(db1$UUID_old))
summary(is.na(db1$standard_filename))

rm(db3)
# fixing UUIDs (no/wrong UUID for release V1.0 ) ============================================

# adding old UUIDs for PAHO PLISA data
db2 <- read.csv(paste0(
  OD_path,
  "PAHO-cum-inc/transformed_data/paho_for_opendengue_V1_0_corrected.csv"
)) %>%
  mutate(raw_data = "V1_0_paho_for_opendengue.csv") %>%
  filter(
    !(ymd(calendar_start_date) > ymd("2021-12-31") &
      ymd(calendar_end_date) < ymd("2024-01-01"))
  )

source_lookup <- db2 %>%
  filter(
    !(is.na(source_id) | !grepl("all_PAHO|PAHO_2022|2014_2024", source_id))
  ) %>%
  group_by(calendar_start_date, calendar_end_date, source_id) %>%
  tally()

db2 <- merge(
  db2,
  source_lookup,
  by = c("calendar_start_date", "calendar_end_date"),
  all.x = T
) %>%
  select(-n) %>%
  rename(source_id = source_id.y) %>%
  mutate(
    source_id = paste0(source_id, ".csv"),
    source_cat = ifelse(
      grepl("all_PAHO|PAHO_2022|2014_2024", source_id.x),
      "PAHO",
      paste0(source_id.x)
    )
  ) %>%
  select(
    adm_0_name:adm_2_code,
    calendar_start_date,
    calendar_end_date,
    dengue_total,
    source_id,
    source_cat,
    raw_data
  )

db2 <- merge(
  db2,
  m[, c("standard_filename", "UUID_old")],
  by.x = "source_id",
  by.y = "standard_filename"
) %>%
  # mutate(UUID = ifelse(source_cat == "paho_imputed", paste0(UUID, " (imputed)"), UUID))%>%
  rename(standard_filename = source_id) %>%
  mutate(original_filename = NA) %>%
  select(names(db1))


# V1.0 asia update
db4 <- read.csv("01_Dengue_data/open_dengue_1.0_asia/open_dengue_asia.csv") %>%
  select(-UUID) %>%
  mutate(source_cat = NA, raw_data = "V1_0_open_dengue_asia.csv") %>% # delete wrong UUID
  filter(!(source_file == "ewars-weekly-bulletin-50th-week-2016.pdf" & adm_2_name == "Sunsari" & dengue_total == 1)) %>% # error from the source; Sunsari was mentioned twice
  filter(!source_file == "profil-kesehatan-indonesia-2010.pdf") # remove indonesia error

idn <- read.csv("01_Dengue_data/open_dengue_1.0_asia/Indonesia_2010_adm1_annual_moh_CORRECTION.csv") %>%
  mutate(source_cat = NA, raw_data = "V1_0_open_dengue_asia.csv")

db4 <- rbind(db4, idn)

# Thailand error correction
db4 <- db4 %>%
  filter(!(adm_0_name == "Thailand" & is.na(adm_1_name))) # remove 265 rows

# add in corrected data
thai_correct <- read.csv(
  "01_Dengue_data/open_dengue_1.0_asia/tycho_thailand_correction.csv"
)

db4 <- rbind(db4, thai_correct)

db4 <- merge(
  db4,
  m[, c("original_filename", "UUID_old")],
  by.x = "source_file",
  by.y = "original_filename"
) %>%
  rename(original_filename = source_file) %>%
  mutate(standard_filename = NA) %>%
  select(names(db1))

db4 <- db4 %>%
  mutate(
    standard_filename = ifelse(
      grepl("Dengue-", original_filename),
      original_filename,
      standard_filename
    )
  )

# standardised data V1.0
data <- rbindlist(list(db1, db2, db4)) %>%
  rename(source_cat_old = source_cat)

rm(db1, db2, db4, thai_correct, idn, source_lookup)

#####################################################################

# V1.1 ==============================================================

#####################################################################

dt <- read.csv(
  "./01_Dengue_data/open_dengue_1.1/open_dengue_update_v1.1.csv"
) %>%
  mutate(raw_data = "V1_1_open_dengue_update_v1.1.csv") %>%
  filter(!(adm_0_name == "Afghanistan" & source_file == "WHO_EMRO_2021.pdf")) # remove wrong data (scraped again in V1.3)

dt <- dt %>% filter(!source_file == "PAHO_2013_adm0_annual.csv") # remove PAHO annual data for 2013

dt <- dt %>%
  mutate(
    source_file = ifelse(
      source_file == "SaudiArabia_2012_annual_statistics.xlsx",
      "SaudiArabia_2012_annual_statistics.pdf",
      ifelse(
        source_file == "SaudiArabia_2019_annual_statistics.pdf",
        "SaudiArabia_2019_annual_statistics.xlsx",
        ifelse(
          source_file == "SaudiArabia_2020_annual_statistics.pdf",
          "SaudiArabia_2019_annual_statistics.xlsx",
          ifelse(
            source_file == "SaudiArabia_2021_annual_statistics.pdf",
            "SaudiArabia_2021_annual_statistics.xlsx",
            source_file
          )
        )
      )
    )
  ) %>%
  mutate(
    UUID_old = NA,
    original_filename = NA,
    source_cat_old = NA,
    adm_0_code = NA,
    adm_1_code = NA,
    adm_2_code = NA
  ) %>%
  rename(standard_filename = source_file) %>%
  select(names(data))

# fix Pakistan date error
dt$calendar_end_date[
  dt$standard_filename == "IDSR-Weekly-Report-52-2021.pdf"
] <- as.character(epiweekToDate(2021, 52)$d1)

dt$calendar_start_date[
  dt$standard_filename == "IDSR-Weekly-Report-52-2021.pdf"
] <- as.character(
  ymd(dt$calendar_end_date[
    dt$standard_filename == "IDSR-Weekly-Report-52-2021.pdf"
  ]) -
    6
)

# fix PSSS 2023 data
dt <- dt %>% filter(!dt$standard_filename == "PSSS_2023_adm0_weekly.pdf")

psss_2023 <- read.csv(
  "01_Dengue_data/open_dengue_1.1/processed_datasets/PSSS_2023_adm0_weekly_CORRECTED.csv"
) %>%
  filter(!is.na(dengue_total)) %>%
  mutate(
    raw_data = "V1_1_open_dengue_update_v1.1.csv",
    standard_filename = source_file,
    UUID_old = NA,
    original_filename = NA,
    source_cat_old = NA
  ) %>%
  select(names(dt))

dt <- rbind(dt, psss_2023)

dt <- merge(
  dt,
  m[, c("standard_filename")],
  by = "standard_filename",
  all.x = T
)

# fix calendar dates for PICs, Pakistan and Palau
dt$date <- paste0(dt$calendar_start_date, "_", dt$calendar_end_date)

dates_fix <- dt %>%
  filter(grepl(
    "PSSS|Palau_2018_2019_adm0_weekly|29-FELTP-Pakistan-Weekly-Epidemiological-Report-Jul-13-19-2020",
    standard_filename
  )) %>%
  distinct(
    adm_0_name,
    standard_filename,
    calendar_start_date,
    calendar_end_date
  ) %>%
  mutate(
    year = year(calendar_start_date),
    week = epiweek(calendar_start_date),
    date = paste0(calendar_start_date, "_", calendar_end_date)
  )

start_year <- year(min(as.Date(dates_fix$calendar_start_date)))
end_year <- year(max(as.Date(dates_fix$calendar_start_date)))
source(paste0(git_path, "scripts/Standard_weekly_dates.R"))
std_dates <- get_std_wdates(start_year, end_year)

dates_fix <- merge(
  dates_fix,
  std_dates[, c("year", "week", "std_start_date", "std_end_date", "std_date")],
  by = c("year", "week"),
  all.x = T
)

dates_fix <- dates_fix[(dates_fix$date != dates_fix$std_date), ] # extract non-standardised dates only

# unique dates to be fixed
dates_fix <- dates_fix %>%
  distinct(
    calendar_start_date,
    calendar_end_date,
    std_start_date,
    std_end_date,
    date,
    std_date
  )

nrow(dt[dt$date %in% dates_fix$date, ]) # 2877 rows to be fixed

dt <- merge(
  dt,
  dates_fix[, c(
    "calendar_start_date",
    "calendar_end_date",
    "std_start_date",
    "std_end_date"
  )],
  by = c("calendar_start_date", "calendar_end_date"),
  all = T
)
summary(is.na(dt$std_start_date))


dt$calendar_start_date <- ifelse(
  !is.na(dt$std_start_date),
  dt$std_start_date,
  dt$calendar_start_date
)
dt$calendar_end_date <- ifelse(
  !is.na(dt$std_end_date),
  dt$std_end_date,
  dt$calendar_end_date
)

dt$date <- paste0(dt$calendar_start_date, "_", dt$calendar_end_date)

nrow(dt[
  grepl(
    "PSSS|Palau_2018_2019_adm0_weekly|29-FELTP-Pakistan-Weekly-Epidemiological-Report-Jul-13-19-2020",
    dt$standard_filename
  ) &
    !(dt$date %in% std_dates$std_date),
]) # checking

# standardised data V1.1
dt <- dt %>% select(names(data))

names(data)
names(dt)

# Merge V1.0 and V1.1
data <- rbind(data, dt)
plyr::count(data$adm_0_name)

rm(dates_fix, epiw, psss_2023, std_dates, dt)

# remove countries with imported cases only
data <- data[
  !data$adm_0_name %in% c("Republic Of Korea", "Canada", "New Zealand"),
]

# for some reason filter did not work correctly
data$adm_2_name_new <- ifelse(
  data$adm_0_name == "Colombia" & data$adm_2_name == "Unknown",
  "delete",
  ifelse(
    data$standard_filename == "Colombia_2007_2017.xlsx" &
      is.na(data$adm_2_name),
    "delete",
    NA
  )
)

unique(data$adm_2_name[data$adm_2_name_new == "delete"])

data <- data[is.na(data$adm_2_name_new)]
data[data$adm_2_name_new == "delete", ]

data <- data %>% select(-adm_2_name_new)

#####################################################################

# V1.3 ==============================================================

#####################################################################

# updating PAHO 2022-2023
db2_new <- read.csv(paste0(
  OD_path,
  "PAHO-cum-inc/transformed_data/paho_for_opendengue_V1_3.csv"
)) %>%
  mutate(raw_data = "V1_3_paho_for_opendengue_V1_3.csv") %>%
  filter(
    ymd(calendar_start_date) > ymd("2021-12-31") &
      ymd(calendar_end_date) < ymd("2025-01-01")
  )

source_lookup <- db2_new %>%
  filter(grepl("2014_2024", source_id)) %>%
  distinct(calendar_start_date, calendar_end_date, source_id) %>%
  transmute(
    calendar_start_date,
    calendar_end_date,
    standard_filename = paste0(source_id, ".csv")
  )

db2_new <- merge(
  db2_new,
  source_lookup,
  by = c("calendar_start_date", "calendar_end_date"),
  all.x = T
)
summary(is.na(db2_new$standard_filename))

db2_new <- db2_new %>%
  mutate(
    source_cat_old = ifelse(
      grepl("imputed|zero", source_id),
      paste0(source_id),
      NA
    ),
    UUID_old = NA,
    original_filename = NA
  ) %>%
  select(names(data))


paho_annual <- read.csv(paste0(
  OD_path,
  "PAHO-cum-inc/transformed_data/paho_for_opendengue_annual_1980_2024.csv"
)) %>%
  filter(!is.na(dengue_total)) %>%
  mutate(
    adm_1_name = NA,
    adm_1_code = NA,
    adm_2_name = NA,
    adm_2_code = NA,
    source_cat_old = NA,
    UUID_old = NA,
    original_filename = NA,
    raw_data = "V1_3_paho_for_opendengue_annual_1980_2024.csv",
    standard_filename = source_id
  ) %>%
  select(names(data))

# WHO Global + SEARO dashboard
who <- read.csv(paste0(
  OD_path,
  "OpenDengue_audit/data/who_for_opendengue.csv"
)) %>%
  mutate(
    adm_0_code = NA,
    adm_1_name = adm_1_name,
    adm_1_code = NA,
    adm_2_name = NA,
    adm_2_code = NA,
    source_cat_old = NA,
    UUID_old = NA,
    original_filename = NA,
    raw_data = "V1_3_who_for_opendengue.csv",
    standard_filename = source_file
  ) %>%
  select(names(data))

# checking who data
who[who$dengue_total < 0]

unique(who$adm_0_name)
# no evidence on local transmission so exclude for now: LIBERIA, SIERRA LEONE
who <- who[!who$adm_0_name %in% c("LIBERIA", "SIERRA LEONE"), ]

# ad hoc data collection
# data sources identified (OJB + AL) and scraped/digitised by KS + PL
adhoc <- read.csv(paste0(
  git_path,
  "open_dengue_1.3/open_dengue_update_v1.3.csv"
)) %>%
  mutate(
    adm_0_code = NA,
    adm_1_name = adm_1_name,
    adm_1_code = NA,
    adm_2_name = adm_2_name,
    adm_2_code = NA,
    source_cat_old = NA,
    UUID_old = NA,
    original_filename = NA,
    raw_data = "V1_3_open_dengue_update_v1.3.csv",
    standard_filename = source_file
  ) %>%
  select(names(data))


# Merge V1.3
data <- rbindlist(list(data, db2_new, paho_annual, who, adhoc))

summary(is.na(data))

rm(db2_new, paho_annual, who, adhoc)


# list of unique std filenames/UUIDs that are included in the dataset:
# both standard filename and UUID available
s_n_all <- data %>% # 507
  filter(!is.na(standard_filename) & !is.na(UUID_old)) %>%
  select(standard_filename, UUID_old) %>%
  distinct()
nrow(s_n_all)

# only std filename available
s_sf_all <- data %>% # 596
  filter(is.na(UUID_old) & !is.na(standard_filename)) %>%
  select(standard_filename) %>%
  distinct()
nrow(s_sf_all)

# only uuid available
s_uuid_all <- data %>% # 90
  filter(is.na(standard_filename) & !is.na(UUID_old)) %>%
  select(UUID_old) %>%
  distinct()
nrow(s_uuid_all)

s_sf_all <- s_sf_all %>% # 594
  filter(!standard_filename %in% s_n_all$standard_filename)
nrow(s_sf_all)

s_uuid_all <- s_uuid_all %>% # 89
  filter(!UUID_old %in% s_n_all$UUID_old)
nrow(s_uuid_all)

s_uuid_all <- rbind(s_uuid_all, s_n_all[, c("UUID_old")])

nrow(s_sf_all) + nrow(s_uuid_all) # 1190 <- this should be equal to released == "Y" in source data

# above is just to merge V1.0 and V1.1 so doesnt need to be repeated in the next update

# source data =====================================================================================
# back to metadata, filter out source files that are not included in the release
m <- m %>%
  mutate(
    sf_released = ifelse(
      standard_filename %in% s_sf_all$standard_filename,
      "Y",
      "N"
    ),
    uuid_released = ifelse(UUID_old %in% s_uuid_all$UUID_old, "Y", "N")
  ) %>%
  mutate(
    released = ifelse(sf_released == "N" & uuid_released == "N", "N", "Y")
  ) %>%
  mutate(released = ifelse(country == "Brazil", "Y", released)) # brazil adm2 data (V1.2 and V1.3) hasnt been incorporated yet

m$metadata_steps <- gsub("rleevant", "relevant", m$metadata_steps)

nrow(m[m$released == "Y", ]) -
  nrow(m[m$released == "Y" & m$country == "Brazil", ]) # 1190

# get country name, source_cat
meta_r <- m %>%
  filter(released == "Y") %>%
  # trimming source_cat and year values
  mutate(
    source_cat = gsub("_", "", source_cat),
    year = gsub("_|-", "-", year)
  ) %>%
  # if the 'year' spans multiple years, separate it into two columns
  separate(year, into = c("year", "year2"), sep = "-") %>%
  mutate(year2 = ifelse(is.na(year2), paste0(year), year2)) %>%
  # get iso country code
  mutate(
    iso3c = countrycode(country, origin = "country.name", destination = "iso3c")
  ) %>%
  mutate(iso3c = ifelse(is.na(iso3c), paste0(country), iso3c))

# serial_cat (W/M/Y): categorize publications based on their frequency of publication
# serial_cat_num: uniquely identify different types of publications within the same frequency category
## for example, weekly publications in Pakistan: `01` for IDSR, `02` for FELTP.
# serial_id: assign a unique identifier to each individual publication

meta_r <- meta_r %>%
  group_by(source_cat, iso3c) %>%
  mutate(
    num_types = n_distinct(metadata_description),
    serial_cat_num = match(metadata_description, unique(metadata_description))
  ) %>%
  ungroup()

# serial_id
meta_r <- meta_r %>%
  group_by(source_cat, iso3c, serial_cat_num) %>%
  arrange(year) %>%
  mutate(
    serial_id = ifelse(serial_cat == "Y", 0:n(), serial_id),
    # period = indicating the years of the report (for the same report type)
    period = ifelse(
      min(as.numeric(year), na.rm = T) < max(as.numeric(year2), na.rm = T),
      paste0(
        min(as.numeric(year), na.rm = T),
        max(as.numeric(year2), na.rm = T)
      ),
      paste0(year)
    )
  ) %>%
  ungroup()

meta_r <- meta_r %>%
  mutate(
    sourceID = paste0(
      source_cat, "-", iso3c, "-", period, "-", serial_cat,
      sprintf("%02d", as.numeric(serial_cat_num))
    )
  ) %>%
  mutate(
    UUID = ifelse(
      !year == year2,
      paste0(sourceID, "-", sprintf("%02d", as.numeric(serial_id))),
      paste0(
        source_cat, "-", iso3c, "-", year, "-", serial_cat,
        sprintf("%02d", as.numeric(serial_cat_num)), "-",
        sprintf("%02d", as.numeric(serial_id))
      )
    )
  )


# checking if any UUIDs have been changed since the last release
f_old <- read_xlsx(paste0(
  git_path,
  "data/archive/filingDB_allV_V1.2.2.xlsx"
)) %>%
  filter(released == "Y") %>%
  rename(UUID_old_v = UUID)

f_all <- merge(
  f_old[, c("UUID_old_v", "standard_filename")],
  meta_r,
  by = c("standard_filename"),
  all = T
)

f_all %>%
  filter(UUID_old_v != UUID) %>%
  select(UUID_old_v, UUID)


# !!!! check if any duplicated UUID
meta_r %>%
  group_by(UUID) %>%
  tally() %>%
  arrange(desc(n))

plyr::count(is.na(meta_r$standard_filename))


###### CASE DEFINITION =========================================

cdf <- read_excel(
  "01_Dengue_data/open_dengue_1.2/Case_definition/case_def.xlsx",
  sheet = 1
) %>%
  # ignore UUID in cdf
  select(
    case_definition_original,
    case_definition_standardised,
    standard_filename
  )

cdf$standard_filename[
  grepl("\\.[^.]+$", cdf$standard_filename) == FALSE
] <- paste0(
  cdf$standard_filename[grepl("\\.[^.]+$", cdf$standard_filename) == FALSE],
  ".pdf"
)

summary(cdf[grepl("\\.[^.]+$", cdf$standard_filename) == FALSE, ])

# fix typos
cdf$standard_filename <- gsub("23..csv", "23.csv", cdf$standard_filename)

# adding case definition
f3_cdf <- read.csv(paste0(git_path, "./open_dengue_1.3/filingDB_V1.3.csv")) %>%
  select(names(cdf))

cdf <- rbind(cdf, f3_cdf)

plyr::count(meta_r$standard_filename %in% cdf$standard_filename) # should be all TRUE
meta_r$standard_filename[!meta_r$standard_filename %in% cdf$standard_filename]

meta_r <- merge(meta_r, cdf, by = c("standard_filename"), all.x = T)

# check any NAs in case definition
summary(is.na(meta_r$case_definition_original))
unique(meta_r$sourceID[is.na(meta_r$case_definition_original)])

meta_r <- meta_r %>%
  select(
    version,
    UUID,
    released,
    source_cat,
    country,
    period,
    case_definition_original,
    case_definition_standardised,
    NOTES,
    original_filename,
    standard_filename,
    UUID_old,
    metadata_description:metadata_steps
  )


# data not included in the release
cdf_nr <- cdf %>%
  filter(!standard_filename %in% meta_r$standard_filename)

meta_nr <- m %>%
  filter(released == "N") %>%
  mutate(source_cat = gsub("_", "", source_cat)) %>%
  mutate(period = gsub("_|-", "", year)) %>%
  merge(., cdf_nr, by = c("standard_filename"), all.x = T) %>%
  mutate(UUID = NA) %>%
  select(
    version,
    UUID,
    released,
    source_cat,
    country,
    period,
    case_definition_original,
    case_definition_standardised,
    NOTES,
    original_filename,
    standard_filename,
    UUID_old,
    metadata_description:metadata_steps
  )

names(meta_r)
names(meta_nr)

meta_all <- rbind(meta_r, meta_nr) %>% arrange(UUID, country, period)

# save filing DB
# !!! if this is the final release, update the file name with the version name
filename_today <- paste0(git_path, "data/archive/filingDB_allV_", today, ".xlsx")

if (release) {
  filename_fd <- gsub(today, "V1.3", filename_today)
} else {
  filename_fd <- filename_today
}

writexl::write_xlsx(meta_all, filename_fd)


# Publish source data (UUID and sourceIDs are only assigned for the released data)

meta_public <- meta_r %>%
  select(
    "UUID",
    "source_cat",
    "country",
    "period",
    "case_definition_original",
    "metadata_description",
    "metadata_url",
    "metadata_steps"
  ) %>%
  arrange(desc(source_cat), country, period)


summary(is.na(meta_public))

# save source data
### !!! check filing DB first
# !!! if this is the final release, update the file name with Version
filename_today <- paste0(git_path, "data/raw_data/sourcedata_", today, ".csv")

if (release) {
  filename_sd <- gsub(today, "V1.3", filename_today)
} else {
  filename_sd <- filename_today
}

write.csv(meta_public, filename_sd, row.names = F)


# attach sourceID and UUID to data
data1 <- merge(
  data[!is.na(data$UUID_old), ],
  meta_all[, c("UUID_old", "case_definition_standardised", "UUID")],
  by = c("UUID_old"),
  all.x = T
) %>%
  mutate(
    UUID = ifelse(
      grepl("zero", source_cat_old),
      paste0(UUID, " (Zero filling)"),
      ifelse(
        grepl("imputed", source_cat_old),
        paste0(UUID, " (Imputed)"),
        paste0(UUID)
      )
    )
  ) %>%
  select(-UUID_old, -standard_filename, -original_filename, -source_cat_old)

data2 <- merge(
  data[is.na(data$UUID_old), ],
  meta_all[, c("standard_filename", "case_definition_standardised", "UUID")],
  by = c("standard_filename"),
  all.x = T
) %>%
  mutate(
    UUID = ifelse(
      grepl("zero", source_cat_old),
      paste0(UUID, " (Zero filling)"),
      ifelse(
        grepl("imputed", source_cat_old),
        paste0(UUID, " (Imputed)"),
        paste0(UUID)
      )
    )
  ) %>%
  select(-UUID_old, -standard_filename, -original_filename, -source_cat_old)


data2[is.na(data2$case_definition_standardised)]


names(data1)
names(data2)
summary(is.na(data1$case_definition_standardised))
summary(is.na(data2$case_definition_standardised))
summary(is.na(data1$UUID))
summary(is.na(data2$UUID))

master <- rbind(data1, data2)

summary(master$dengue_total)
summary(is.na(master))

# missing calendar end date
# master$calendar_end_date <- ifelse(is.na(master$calendar_end_date), format(as.Date(master$calendar_start_date)+6,  "%Y-%m-%d"),  format(as.Date(master$calendar_end_date), "%Y-%m-%d"))

unique(master$adm_0_name[is.na(master$dengue_total)])
unique(master$UUID[is.na(master$dengue_total)])

# missing dengue_total - manual edits
master$dengue_total[
  master$adm_0_name == "China" & is.na(master$dengue_total)
] <- 62

# remove NAs from Philippines, India, Haiti
master <- master[!is.na(master$dengue_total)]

master[master$dengue_total < 0, ]
# master <- master %>% mutate(dengue_total = ifelse(dengue_total < 0, 0, as.numeric(dengue_total)))
summary(master$dengue_total) # !!! Check any NAs / abnormal values

# check any duplicated rows
dup <- master %>%
  group_by(
    adm_0_name,
    adm_1_name,
    adm_2_name,
    calendar_start_date,
    calendar_end_date,
    dengue_total,
    UUID
  ) %>%
  mutate(dup1 = ifelse(n() > 1, TRUE, FALSE)) %>%
  filter(dup1 == TRUE) %>%
  tally()
nrow(dup) # 41 (PICs)

unique(dup$UUID)

# original PSSS weekly reports date error (w5: 1/29-2/4 and w6: 1/30-2/5)
# dates fix resulted in duplicated counts
# duplicates will be handeled in geomatch extract builder

master2 <- master %>%
  group_by(
    adm_0_name,
    adm_0_code,
    adm_1_name,
    adm_1_code,
    adm_2_name,
    adm_2_code,
    calendar_start_date,
    calendar_end_date,
    dengue_total,
    UUID
  ) %>%
  distinct() %>%
  ungroup()

# save master DB
# !!! if this is the final release, update the file name with Version
filename_today <- paste0(git_path, "data/raw_data/masterDB_", today, ".csv")

if (release) {
  filename_md <- gsub(today, "V1.3", filename_today)
} else {
  filename_md <- filename_today
}

write.csv(master2, filename_md, row.names = F)


rm(list = setdiff(ls(), c("master2", "git_path", "today")))


# ==============================================================
# checking if any UUIDs have been changed since the last release
# if old UUIDs should be replaced with new UUIDs then change the file names
f_old <- read_xlsx(paste0(
  git_path,
  "data/archive/filingDB_allV_V1.2.2.xlsx"
)) %>%
  filter(released == "Y") %>%
  rename(UUID_old_v = UUID)
f_new <- read_xlsx(filename_md) %>% filter(released == "Y")

f_all <- merge(
  f_old[, c("UUID_old_v", "standard_filename")],
  f_new,
  by = c("standard_filename"),
  all = T
) %>%
  filter(UUID_old_v != UUID)

f_all$ext <- sub(".*(\\.[^.]+)$", "\\1", f_all$standard_filename)
f_all$old_name <- paste0(
  "01_Dengue_data/source_files/UUID/",
  f_all$UUID_old_v,
  f_all$ext
)
f_all$new_name <- paste0(
  "01_Dengue_data/source_files/UUID/",
  f_all$UUID,
  f_all$ext
)


for (i in 1:nrow(f_all)) {
  if (file.exists(f_all$old_name[i])) {
    # copy files from current file location to desired location
    file.copy(f_all$old_name[i], "C:/Users/AhyoungLim/Desktop")
    file.rename(f_all$old_name[i], f_all$new_name[i])
  } else {
    print(paste("File Not found :", basename(f_all$old_name[i]), "th row"))
  }
}

# f1 <- read_excel("./01_Dengue_data/open_dengue_update_1.1/new_data_sources.xlsx", sheet=1)
# names(f1)
# dt <- read.csv("./01_Dengue_data/open_dengue_update_1.1/open_dengue_update_v1.1.csv")
#
# f1 <- dt %>%
#   group_by(source_file)%>%
#   tally()%>%
#   merge(., f1, by.x="source_file", by.y="source_file_name", all.x=T, all.y=T)%>%
#   select(source_file, source_name, Country,  Time_span, link, step )%>%
#   rename(standard_filename = source_file,
#          source_cat = source_name,
#          country= Country,
#          year = Time_span ,
#          metadata_url = link,
#          metadata_steps = step
#
#          )%>%
#   mutate(serial_cat = "",
#          serial_id = "",
#          metadata_description = ""
#          )%>%
#   select(standard_filename:year, serial_cat, serial_id, metadata_description, metadata_url, metadata_steps)
#
#
# writexl::write_xlsx(f1, "./01_Dengue_data/open_dengue_update_1.1/filingDB_V1.1.xlsx") # export and manually fill it

# dt2 <- read_excel(dat, sheet=2)
#
# m <- read.csv("C:/Users/AhyoungLim/Dropbox/WORK/Dengue Project 2022/Dengue DB/master-repo/data/raw_data/master_data.csv")
#
# asia <- read.csv("C:/Users/AhyoungLim/Dropbox/WORK/Dengue Project 2022/11_DENData/01_Dengue_data/open_dengue_asia_1.0/metadata_asia.csv")
#
# dt$UUID_in_v1 <- ifelse(dt$UUID %in% m$UUID, "1", "0")
# dt2$UUID_in_v1 <- ifelse(dt2$standard_filename %in% asia$standard_filename , "1", "0")
# write.csv(dt2, "C:/Users/AhyoungLim/Desktop/metadata_V1.0_2.csv",row.names=F)
# names(dt)
# dt <- dt %>%
#   mutate(source_cat = gsub("_", "", source_cat),
#          year = gsub("_", "", year))%>%
#   # mutate(iso3c = countrycode(country, origin = "country.name", destination = "iso3c"))%>%
#   mutate(iso3c = ifelse(country == "ALL", "ALL", iso3c))%>%
#  mutate(SourceID = paste0(source_cat, "-", iso3c, "-", year, "-", serial_cat, sprintf("%02d", as.numeric(serial_id))))
#
# write.csv(dt, "C:/Users/AhyoungLim/Desktop/metadata_V1.0.csv",row.names=F)
