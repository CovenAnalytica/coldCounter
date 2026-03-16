library(httr)
library(rvest)
library(dplyr)
library(stringr)
library(purrr)
library(tibble)
library(tidygeocoder)
library(sf)
library(sfarrow)
library(tigris)

# --- Load AOR shapefile ---

aor_sf <- sfarrow::st_read_feather("data/ice-aor-shp.feather")

# --- Scrape ICE offices ---

# functions to scrape field offices and sub-offices
scrape_field_offices <- function(url) {
  results <- GET(url)
  page <- read_html(content(results, as = "text"))

  # get content for each field office
  field_offices <- page |> html_elements(".grid__content")

  parse_field_office <- function(field_office) {
    area <- field_office |>
      html_element(".views-field.views-field-body .field-content") |>
      html_text(trim = TRUE)
    office_name <- field_office |>
      html_element(".views-field.views-field-title") |>
      html_text(trim = TRUE)
    office_type <- field_office |>
      html_element(".field-content") |>
      html_text(trim = TRUE)
    address_line_1 <- field_office |>
      html_element(".address-line1") |>
      html_text(trim = TRUE)
    address_line_2 <- field_office |>
      html_element(".address-line2") |>
      html_text(trim = TRUE)
    city <- field_office |> html_element(".locality") |> html_text(trim = TRUE)
    state <- field_office |>
      html_element(".administrative-area") |>
      html_text(trim = TRUE)
    zip <- field_office |>
      html_element(".postal-code") |>
      html_text(trim = TRUE)

    # clean area
    area <- if (
      !is.na(area) &&
        str_detect(area, regex("Area of Responsibility:", ignore_case = TRUE))
    ) {
      str_match(
        area,
        regex(
          "Area of Responsibility:\\s*(.*?)(?:\\s*Email:|$)",
          dotall = TRUE,
          ignore_case = TRUE
        )
      )[, 2] |>
        str_squish()
    } else {
      NA
    }

    tibble(
      area = area,
      office_name = office_name,
      office_name_short = office_type,
      agency = str_replace(office_type, ".*-\\s*", ""),
      address = str_replace(
        ifelse(
          is.na(address_line_2),
          address_line_1,
          paste(address_line_1, address_line_2)
        ),
        "^[^0-9]*",
        ""
      ),
      city = city,
      state = state,
      zip_4 = zip,
      zip = str_extract(zip, "^\\d{5}")
    )
  }

  # bind into one tibble
  field_office_df <- map_dfr(field_offices, parse_field_office)

  return(field_office_df)
}

scrape_sub_offices <- function(url) {
  results <- GET(url)
  page <- read_html(content(results, as = "text"))

  # get content for each sub-office
  sub_offices <- page |> html_elements(".grid__content")

  parse_sub_office <- function(sub_office) {
    area <- sub_office |>
      html_element(".views-field.views-field-body .field-content") |>
      html_text(trim = TRUE)
    office_name <- sub_office |>
      html_element(".views-field.views-field-field-field-office-location") |>
      html_text(trim = TRUE)
    field_office_name <- sub_office |>
      html_element(".views-field.views-field-field-field-office-name") |>
      html_text(trim = TRUE)
    address_line_1 <- sub_office |>
      html_element(".address-line1") |>
      html_text(trim = TRUE)
    address_line_2 <- sub_office |>
      html_element(".address-line2") |>
      html_text(trim = TRUE)
    city <- sub_office |> html_element(".locality") |> html_text(trim = TRUE)
    state <- sub_office |>
      html_element(".administrative-area") |>
      html_text(trim = TRUE)
    zip <- sub_office |> html_element(".postal-code") |> html_text(trim = TRUE)

    # clean area
    area <- if (
      !is.na(area) &&
        str_detect(area, regex("Area Coverage:", ignore_case = TRUE))
    ) {
      str_match(
        area,
        regex(
          "Area Coverage:\\s*(.*?)(?:\\s*Appointment Times:|$)",
          dotall = TRUE,
          ignore_case = TRUE
        )
      )[, 2] |>
        str_squish()
    } else {
      NA
    }

    tibble(
      area = area,
      office_name = office_name,
      agency = "ERO",
      field_office_name = field_office_name,
      address = str_replace(
        ifelse(
          is.na(address_line_2),
          address_line_1,
          paste(address_line_1, address_line_2)
        ),
        "^[^0-9]*",
        ""
      ),
      city = city,
      state = state,
      zip_4 = zip,
      zip = str_extract(zip, "^\\d{5}")
    )
  }

  # bind into one tibble
  sub_office_df <- map_dfr(sub_offices, parse_sub_office)

  return(sub_office_df)
}

# number of pages helper function
get_num_pages <- function(url) {
  results <- GET(url)
  page <- read_html(content(results, as = "text"))

  pages <- page |>
    html_element(".usa-pagination") |>
    html_text(trim = TRUE)

  page_nums <- str_extract_all(pages, "\\d+") |>
    unlist() |>
    as.integer()

  if (length(page_nums) == 0) {
    return(1)
  } else {
    return(max(page_nums))
  }
}

# get total number of pages
n_field_pages <- get_num_pages("https://www.ice.gov/contact/field-offices")
n_sub_pages <- get_num_pages("https://www.ice.gov/contact/check-in")

# scrape both types of offices
field_offices <- map_dfr(0:(n_field_pages - 1), function(i) {
  url <- paste0("https://www.ice.gov/contact/field-offices?page=", i)
  Sys.sleep(1)
  scrape_field_offices(url)
}) |>
  distinct()

field_offices <-
  # replace "St Paul" with "St. Paul" for consistency
  field_offices |>
  mutate(
    office_name = str_replace(office_name, "St Paul", "St. Paul")
  )

sub_offices <- map_dfr(0:(n_sub_pages - 1), function(i) {
  url <- paste0("https://www.ice.gov/contact/check-in?page=", i)
  Sys.sleep(1)
  scrape_sub_offices(url)
})

# combine into one dataframe
all_offices <-
  bind_rows(
    `FALSE` = field_offices,
    `TRUE` = sub_offices,
    .id = "sub_office"
  ) |>
  mutate(sub_office = as.logical(sub_office)) |>
  transmute(
    office_name,
    office_name_short,
    agency,
    field_office_name,
    sub_office,
    address,
    city,
    state,
    zip,
    zip_4,
    address_full = str_c(address, ", ", city, ", ", state, " ", zip_4),
    area
  )

# get geocoding from last data, geocode only offices with new or changed addresses
existing_offices <- sfarrow::st_read_feather(
  "data/ice-offices-shp.feather"
)

new_offices <- anti_join(all_offices, existing_offices, by = "address_full")

if (nrow(new_offices) > 0) {
  new_offices_geocoded <-
    new_offices |>
    geocode(
      address = address_full,
      method = "arcgis",
      lat = office_latitude,
      long = office_longitude
    ) |>
    st_as_sf(
      coords = c("office_longitude", "office_latitude"),
      crs = 4326,
      remove = FALSE,
      agr = "constant",
      na.fail = FALSE,
      sf_column_name = "geometry_office"
    )
}

offices_geocoded <-
  bind_rows(
    if (nrow(new_offices) > 0) {
      st_drop_geometry(new_offices_geocoded) |>
        select(address_full, office_latitude, office_longitude)
    } else {
      tibble()
    },
    existing_offices |>
      st_drop_geometry() |>
      select(address_full, office_latitude, office_longitude)
  ) |>
  filter(!is.na(office_latitude) & !is.na(office_longitude)) |>
  distinct(address_full, office_latitude, office_longitude)

# combine with existing geocoded offices
library(tidylog)

all_offices_geocoded <-
  all_offices |>
  left_join(offices_geocoded |> distinct(address_full, .keep_all = TRUE)) |>
  mutate(
    area_of_responsibility_name = case_when(
      office_name == "Miramar Sub Office" & sub_office == FALSE ~ "Miami", # field office moved to Miramar
      office_name == "St. Paul Field Office" & sub_office == FALSE ~ "St Paul", # field office renamed moved to St. Paul
      agency == "ERO" & !sub_office ~ str_remove(office_name, " Field Office"),
      TRUE ~ NA_character_
    )
  ) |>
  left_join(
    aor_sf |>
      select(-office_name) |>
      as_tibble() |>
      rename(geometry_aor = geometry),
    by = c("area_of_responsibility_name")
  ) |>
  select(-area_of_responsibility_name) |>
  st_as_sf(
    coords = c("office_longitude", "office_latitude"),
    crs = 4326,
    remove = FALSE,
    agr = "constant",
    na.fail = FALSE,
    sf_column_name = "geometry_office"
  )

sfarrow::st_write_feather(
  all_offices_geocoded,
  "data/ice-offices-shp.feather"
)

arrow::write_feather(
  all_offices_geocoded |>
    st_drop_geometry() |>
    select(-contains("geometry_")),
  "data/ice-offices.feather"
)

# save as xlsx
writexl::write_xlsx(
  all_offices_geocoded |>
    st_drop_geometry() |>
    select(-contains("geometry_")),
  path = "data/ice-offices.xlsx"
)

# save as dta
haven::write_dta(
  all_offices_geocoded |>
    st_drop_geometry() |>
    select(-contains("geometry_")),
  path = "data/ice-offices.dta"
)

# save as sav
haven::write_sav(
  all_offices_geocoded |>
    st_drop_geometry() |>
    select(-contains("geometry_")),
  path = "data/ice-offices.sav"
)

temp_dir <- tempdir()
temp_shp_path <- file.path(temp_dir, "ice-offices.shp")
st_write(all_offices_geocoded, temp_shp_path, append = FALSE)

# create a zip file with all necessary shapefile components
if (file.exists("data/ice-offices-shp.zip")) {
  file.remove("data/ice-offices-shp.zip")
}
zip(
  zipfile = "data/ice-offices-shp.zip",
  files = c(
    file.path(temp_dir, "ice-offices.shp"),
    file.path(temp_dir, "ice-offices.dbf"),
    file.path(temp_dir, "ice-offices.prj"),
    file.path(temp_dir, "ice-offices.shx")
  ),
  flags = "-j"
)