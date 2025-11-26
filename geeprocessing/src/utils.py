import ee


def export_seasonal_weather_stats(
    year,
    region_fc,
    region_name,
    region_id_property,
    scale=9000,
    folder_name="GEE_WEATHER_EXPORTS",
    apply_gap_filling=True,
    start_month=1,
    end_month=12,
):
    """
    Processes hourly ERA5 data to produce and export seasonal weather statistics for a given year and region.

    ```
    For each season (winter, spring, summer, autumn), this function calculates:
    1. Aggregate statistics (mean, median, sum) for core weather variables.
    2. Hourly exposure counts for temperature (-50C to +50C) and wind speed (0 to 25 m/s) bins.

    The results are exported as a single CSV file for the year to Google Drive.

    Parameters:
    - year (int): Target year.
    - region_fc (ee.FeatureCollection): Boundaries to aggregate data over.
    - region_name (str): Name for the region, used in output filename.
    - region_id_property (str): Unique ID property for each feature.
    - scale (int): Scale in meters for spatial reduction.
    - folder_name (str): Google Drive folder for output.
    - start_month (int): Starting month (default: 1)
    - end_month (int): Ending month (default: 12)
    """

    # --- Define Seasons ---
    seasons = {
        "aut": {"months": [9, 10, 11], "name": "aut"},
        "spr": {"months": [3, 4, 5], "name": "spr"},
        "smr": {"months": [6, 7, 8], "name": "smr"},
        "wtr": {"months": [12, 1, 2], "name": "wtr"},
    }

    # --- Define Variables and Bins ---
    core_variables = [
        "temperature_2m",
        "snow_cover",
        "snow_density",
        "snow_depth",
        "snowfall",
        "snowmelt",
        "total_precipitation",
        "u_component_of_wind_10m",
        "v_component_of_wind_10m",
    ]
    derived_variables = ["temperature_c", "wind_speed", "wind_direction"]

    temp_bins = ee.List.sequence(-50, 50)
    wind_bins = ee.List.sequence(0, 25)

    # --- Get Base ERA5 Collection ---
    era5_collection = ee.ImageCollection(
        "ECMWF/ERA5_LAND/HOURLY"
    ).filterBounds(region_fc.geometry())

    # --- Apply gap filling if requested ---
    if apply_gap_filling:
        gap_fill_start = ee.Date.fromYMD(year, start_month, 1).advance(
            -1, "month"
        )
        gap_fill_end = (
            ee.Date.fromYMD(year, end_month + 1, 1)
            if end_month < 12
            else ee.Date.fromYMD(year + 1, 1, 1).advance(1, "month")
        )

        full_year_collection = era5_collection.filterDate(
            gap_fill_start, gap_fill_end
        ).select(core_variables)
        gap_filled_collection = apply_temporal_gap_filling(
            full_year_collection, region_fc.geometry(), core_variables
        )
        core_variables_filled = [v + "_filled" for v in core_variables]
    else:
        gap_filled_collection = None
        core_variables_filled = core_variables

    yearly_results = []

    for season_code, params in seasons.items():
        # --- Handle Date Ranges (especially winter) ---
        if season_code == "wtr":
            start_date = ee.Date.fromYMD(year - 1, params["months"][0], 1)
            end_date = ee.Date.fromYMD(year, params["months"][-1], 1).advance(
                1, "month"
            )
        else:
            start_date = ee.Date.fromYMD(year, params["months"][0], 1)
            end_date = ee.Date.fromYMD(year, params["months"][-1], 1).advance(
                1, "month"
            )

        # --- Filter and Pre-process Data ---
        if apply_gap_filling and gap_filled_collection:
            seasonal_data = gap_filled_collection.filterDate(
                start_date, end_date
            )
            seasonal_data = seasonal_data.map(
                lambda img: img.select(core_variables_filled, core_variables)
            )
        else:
            seasonal_data = era5_collection.filterDate(
                start_date, end_date
            ).select(core_variables)

        processed_seasonal_data = seasonal_data.map(process_era5_image)

        # --- 1. Aggregate Statistics (Mean, Median, Sum) ---
        stat_reducer = (
            ee.Reducer.mean()
            .combine(ee.Reducer.median(), "", True)
            .combine(ee.Reducer.sum(), "", True)
        )

        bands_for_stats = core_variables + derived_variables
        stats_image = processed_seasonal_data.select(bands_for_stats).reduce(
            stat_reducer
        )

        # --- 2. Exposure Bin Counts ---
        def get_bin_counts(image):
            temp = image.select("temperature_c").round()
            wind = image.select("wind_speed").round()

            temp_bin_images = temp_bins.map(
                lambda t: temp.eq(ee.Number(t)).rename(
                    ee.String("temp_h_").cat(ee.Number(t).int().format())
                )
            )
            temp_bands = ee.ImageCollection.fromImages(
                temp_bin_images
            ).toBands()
            temp_band_names = temp_bins.map(
                lambda t: ee.String("temp_h_").cat(ee.Number(t).int().format())
            )
            temp_bands = temp_bands.rename(temp_band_names)

            wind_bin_images = wind_bins.map(
                lambda w: wind.eq(ee.Number(w)).rename(
                    ee.String("wind_h_").cat(ee.Number(w).int().format())
                )
            )
            wind_bands = ee.ImageCollection.fromImages(
                wind_bin_images
            ).toBands()
            wind_band_names = wind_bins.map(
                lambda w: ee.String("wind_h_").cat(ee.Number(w).int().format())
            )
            wind_bands = wind_bands.rename(wind_band_names)

            return temp_bands.addBands(wind_bands)

        binned_images = processed_seasonal_data.map(get_bin_counts)
        bins_image = binned_images.reduce(ee.Reducer.sum())

        # --- 3. Combine Stats + Bins ---
        final_image = stats_image.addBands(bins_image)

        new_band_names = final_image.bandNames().map(
            lambda b: ee.String(b).cat("_").cat(season_code)
        )
        final_image_renamed = final_image.rename(new_band_names)
        yearly_results.append(final_image_renamed)

    # --- Combine all seasonal results ---
    yearly_image = ee.Image.cat(yearly_results)

    # --- Split bins vs stats bands ---
    all_band_names = yearly_image.bandNames()

    # Bands with "_h_" are the bin counts; others are stats
    bin_bands = all_band_names.filter(ee.Filter.stringContains("item", "_h_"))
    stat_bands = all_band_names.filter(
        ee.Filter.Not(ee.Filter.stringContains("item", "_h_"))
    )

    bins_image = yearly_image.select(bin_bands)
    stats_image = yearly_image.select(stat_bands)

    # --- ReduceRegions: mean for stats ---
    stats_fc = stats_image.reduceRegions(
        collection=region_fc.select([region_id_property]),
        reducer=ee.Reducer.mean(),
        scale=scale,
        tileScale=1,
    )

    # --- ReduceRegions: sum for bins ---
    bins_fc = bins_image.reduceRegions(
        collection=region_fc.select([region_id_property]),
        reducer=ee.Reducer.sum(),
        scale=scale,
        tileScale=1,
    )

    # --- Join both results by region_id_property ---
    join_filter = ee.Filter.equals(
        leftField=region_id_property,
        rightField=region_id_property,
    )

    joined = ee.Join.inner().apply(stats_fc, bins_fc, join_filter)

    # Merge properties: copy stats + bins into single feature per region
    final_fc = joined.map(
        lambda f: ee.Feature(f.get("primary")).copyProperties(
            f.get("secondary")
        )
    )

    # --- Export ---
    month_suffix = (
        f"_m{start_month:02d}-{end_month:02d}"
        if start_month != 1 or end_month != 12
        else ""
    )
    output_filename = f"weather_stats_{region_name}_{year}{month_suffix}"

    task = ee.batch.Export.table.toDrive(
        collection=final_fc,
        description=output_filename,
        folder=folder_name,
        fileNamePrefix=output_filename,
        fileFormat="CSV",
    )

    task.start()
    print(
        f"Started seasonal export for {region_name} {year}. Filename: {output_filename}"
    )