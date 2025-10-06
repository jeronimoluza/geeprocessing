# coding=utf-8

import ee
import math


def shift_collection(ic, offset_hours):
    """
    Create a shifted collection by advancing/retreating the timestamp of each image.
    Used for temporal joins in gap-filling.
    """
    return ic.map(
        lambda img: img.set(
            "shifted_time", img.date().advance(offset_hours, "hour")
        )
    )


def temporal_join(
    primary, secondary, match_key="shifted_time", save_key="match"
):
    """
    Join primary collection with secondary based on matching timestamps.
    Used to attach temporal neighbors for gap-filling.
    """
    return ee.Join.saveFirst(
        matchKey=save_key, ordering="system:time_start", outer=True
    ).apply(
        primary,
        secondary,
        ee.Filter.equals(leftField="system:time_start", rightField=match_key),
    )


def apply_temporal_gap_filling(collection, region, variables=None):
    """
    Apply temporal gap-filling strategy as outlined in bbff_reqs.md:
    1. Try backward fill: prev1 -> prev2 -> prev3
    2. Try forward fill: next1 -> next2 -> next3
    3. Fall back to spatial mean within region

    Parameters:
    - collection: ee.ImageCollection to fill gaps in
    - region: ee.Geometry or ee.FeatureCollection for spatial mean fallback
    - variables: list of band names to apply gap filling to (if None, applies to all)

    Returns:
    - ee.ImageCollection with gap-filled bands
    """

    if variables is None:
        # Use hardcoded ERA5 variables to avoid string conversion issues
        variables = [
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
    elif not isinstance(variables, list):
        # Convert ee.List to Python list to avoid string conversion issues
        variables = [
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

    # Convert collection to list for easier temporal neighbor access
    collection_list = collection.sort("system:time_start").toList(
        collection.size()
    )
    collection_size = collection.size()

    def fill_temporal_gaps(index):
        """
        Fill gaps for image at given index using temporal neighbors.
        """
        index = ee.Number(index)
        img = ee.Image(collection_list.get(index))

        # Get temporal neighbors with bounds checking
        def get_neighbor_safe(offset):
            neighbor_index = index.add(offset)
            return ee.Image(
                ee.Algorithms.If(
                    neighbor_index.gte(0).And(
                        neighbor_index.lt(collection_size)
                    ),
                    collection_list.get(neighbor_index),
                    # Create a masked image with the same band structure as the base image
                    img.select(variables).updateMask(0),
                )
            )

        # Get temporal neighbors
        prev1 = get_neighbor_safe(-1)
        prev2 = get_neighbor_safe(-2)
        prev3 = get_neighbor_safe(-3)
        next1 = get_neighbor_safe(1)
        next2 = get_neighbor_safe(2)
        next3 = get_neighbor_safe(3)

        # Start with original image
        base = img.select(variables)

        # Apply hierarchical filling: backward fill first, then forward fill
        filled = base
        filled = filled.unmask(prev1.select(variables))
        filled = filled.unmask(prev2.select(variables))
        filled = filled.unmask(prev3.select(variables))
        filled = filled.unmask(next1.select(variables))
        filled = filled.unmask(next2.select(variables))
        filled = filled.unmask(next3.select(variables))

        # For any remaining missing values, use spatial mean fallback
        def apply_spatial_fallback(band_name):
            # Don't convert to ee.String - use band_name directly
            band_img = filled.select([band_name])

            # Calculate spatial mean for this band
            spatial_mean = (
                img.select([band_name])
                .reduceRegion(
                    reducer=ee.Reducer.mean(),
                    geometry=region,
                    scale=10000,
                    bestEffort=True,
                    maxPixels=1e9,
                )
                .get(band_name)
            )

            # Fill remaining nulls with spatial mean
            filled_band = band_img.unmask(ee.Image.constant(spatial_mean))
            return filled_band.rename(band_name)

        # Apply spatial fallback to each variable - handle each band individually
        def process_band(band_name):
            return apply_spatial_fallback(band_name)

        # Since variables is now a Python list, process directly
        filled_bands_list = []
        for var in variables:
            filled_bands_list.append(process_band(var))

        # Combine all bands into a single image
        filled_image = ee.Image.cat(filled_bands_list)

        # Add filled bands to original image with '_filled' suffix
        # Use simple string concatenation for band renaming
        if isinstance(variables, list):
            # If variables is a Python list, create filled names directly
            filled_names = [f"{var}_filled" for var in variables]
        else:
            # If variables is an ee.List, convert to simple list approach
            filled_names = [
                "temperature_2m_filled",
                "snow_cover_filled",
                "snow_density_filled",
                "snow_depth_filled",
                "snowfall_filled",
                "snowmelt_filled",
                "total_precipitation_filled",
                "u_component_of_wind_10m_filled",
                "v_component_of_wind_10m_filled",
            ]

        filled_renamed = filled_image.rename(filled_names)

        # Preserve original image properties
        return img.addBands(filled_renamed).copyProperties(
            img, ["system:time_start"]
        )

    # Apply gap filling to each image in the collection
    indices = ee.List.sequence(0, collection_size.subtract(1))
    filled_list = indices.map(fill_temporal_gaps)

    return ee.ImageCollection.fromImages(filled_list)


def process_era5_image(image):
    """
    Calculates derived variables for a single ERA5 image.
    - Converts temperature from Kelvin to Celsius.
    - Calculates wind speed and direction.
    """
    # Convert temperature to Celsius
    temp_c = (
        image.select("temperature_2m").subtract(273.15).rename("temperature_c")
    )

    # Calculate wind speed from U and V components
    u_wind = image.select("u_component_of_wind_10m")
    v_wind = image.select("v_component_of_wind_10m")
    wind_speed = (u_wind.pow(2).add(v_wind.pow(2))).sqrt().rename("wind_speed")

    # Calculate wind direction (in degrees)
    wind_dir = (
        ee.Image(180)
        .add(ee.Image(180).divide(math.pi).multiply(v_wind.atan2(u_wind)))
        .rename("wind_direction")
    )

    return image.addBands([temp_c, wind_speed, wind_dir])


def export_hourly_weather_data(
    year,
    region_fc,
    region_name,
    region_id_property,
    scale=9000,
    folder_name="GEE_WEATHER_OUTPUT",
    apply_gap_filling=True,
    start_month=1,
    end_month=12,
):
    """
    Export cleaned hourly weather data for a given year and region.
    This produces the "Cleaned Hourly Data" output as specified in project requirements.

    Parameters:
    - year (int): The target year for processing
    - region_fc (ee.FeatureCollection): Administrative boundaries
    - region_name (str): Name for the region (e.g., 'MNG', 'CHN_IM')
    - region_id_property (str): Property holding unique ID for each feature
    - scale (int): Scale in meters for spatial reduction
    - folder_name (str): Google Drive folder for export
    - apply_gap_filling (bool): Whether to apply temporal gap-filling
    - start_month (int): Starting month for processing (default: 1)
    - end_month (int): Ending month for processing (default: 12)
    """

    # Define core variables to process
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

    # Get ERA5 collection for the specified month range
    start_date = ee.Date.fromYMD(year, start_month, 1)
    end_date = (
        ee.Date.fromYMD(year, end_month + 1, 1)
        if end_month < 12
        else ee.Date.fromYMD(year + 1, 1, 1)
    )

    era5_collection = (
        ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")
        .filterBounds(region_fc.geometry())
        .filterDate(start_date, end_date)
        .select(core_variables)
    )

    # Apply gap filling if requested
    if apply_gap_filling:
        # Expand date range for gap filling context (like in export_seasonal_weather_stats)
        gap_fill_start = start_date.advance(
            -1, "month"
        )  # One month before for context
        gap_fill_end = end_date.advance(
            1, "month"
        )  # One month after for context

        # Get expanded collection for gap filling
        gap_fill_collection = (
            ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")
            .filterBounds(region_fc.geometry())
            .filterDate(gap_fill_start, gap_fill_end)
            .select(core_variables)
        )

        # Apply temporal gap filling to expanded collection
        gap_filled_collection = apply_temporal_gap_filling(
            gap_fill_collection, region_fc.geometry(), core_variables
        )

        # Filter back to original date range after gap filling
        era5_collection = gap_filled_collection.filterDate(
            start_date, end_date
        )

        # Use gap-filled bands
        variables_to_process = [v + "_filled" for v in core_variables]
    else:
        variables_to_process = core_variables

    # Process images to add derived variables
    def process_hourly_image(img):
        # Select the appropriate bands (filled or original)
        if apply_gap_filling:
            # Rename filled bands back to original names for processing
            processed = img.select(variables_to_process, core_variables)
        else:
            processed = img.select(variables_to_process)

        # Apply standard processing (temperature conversion, wind calculations)
        processed = process_era5_image(processed)

        # Add time information
        processed = processed.set(
            {
                "year": img.date().get("year"),
                "month": img.date().get("month"),
                "day": img.date().get("day"),
                "hour": img.date().get("hour"),
                "system:time_start": img.get("system:time_start"),
            }
        )

        return processed

    processed_collection = era5_collection.map(process_hourly_image)

    # Reduce to regions and export
    def reduce_to_regions(img):
        reduced = img.reduceRegions(
            collection=region_fc.select([region_id_property]),
            reducer=ee.Reducer.mean(),
            scale=scale,
            tileScale=4,
        )

        # Add time properties to each feature
        def add_time_props(feature):
            return feature.set(
                {
                    "year": img.get("year"),
                    "month": img.get("month"),
                    "day": img.get("day"),
                    "hour": img.get("hour"),
                    "system:time_start": img.get("system:time_start"),
                }
            )

        return reduced.map(add_time_props)

    # Apply reduction and flatten
    hourly_fc = processed_collection.map(reduce_to_regions).flatten()

    # Export to CSV
    month_suffix = (
        f"_m{start_month:02d}-{end_month:02d}"
        if start_month != 1 or end_month != 12
        else ""
    )
    output_filename = f"weather_hourly_{region_name}_{year}{month_suffix}"

    task = ee.batch.Export.table.toDrive(
        collection=hourly_fc,
        description=output_filename,
        folder=folder_name,
        fileNamePrefix=output_filename,
        fileFormat="CSV",
    )

    task.start()
    print(
        f"Started hourly export task for {region_name} {year}. Filename: {output_filename}"
    )


def export_seasonal_weather_stats(
    year,
    region_fc,
    region_name,
    region_id_property,
    scale=9000,
    folder_name="GEE_WEATHER_OUTPUT",
    apply_gap_filling=True,
    start_month=1,
    end_month=12,
):
    """
    Processes hourly ERA5 data to produce and export seasonal weather statistics for a given year and region.

    For each season (winter, spring, summer, autumn), this function calculates:
    1. Aggregate statistics (mean, median, sum) for core weather variables.
    2. Hourly exposure counts for temperature (-50C to +50C) and wind speed (0 to 25 m/s) bins.

    The results are exported as a single CSV file for the year to Google Drive.

    Parameters:
    - year (int): The target year for processing.
    - region_fc (ee.FeatureCollection): The administrative boundaries to aggregate data over.
    - region_name (str): A name for the region, used in the output filename (e.g., 'MNG', 'CHN_IM').
    - region_id_property (str): The property in the FeatureCollection that holds the unique ID for each feature (e.g., 'GID_2').
    - scale (int): The scale in meters for the spatial reduction. Defaults to 9000.
    - folder_name (str): The Google Drive folder to export the CSV files to.
    - start_month (int): Starting month for processing (default: 1)
    - end_month (int): Ending month for processing (default: 12)
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

    temp_bins = ee.List.sequence(-50, 50)  # Bins from -50 to +50
    wind_bins = ee.List.sequence(0, 25)  # Bins from 0 to 25

    # --- Get Base ERA5 Collection ---
    era5_collection = ee.ImageCollection(
        "ECMWF/ERA5_LAND/HOURLY"
    ).filterBounds(region_fc.geometry())

    # Apply gap filling to the specified month range if requested
    if apply_gap_filling:
        # Adjust date range based on month filtering
        gap_fill_start = ee.Date.fromYMD(year, start_month, 1).advance(
            -1, "month"
        )  # One month before for context
        gap_fill_end = (
            ee.Date.fromYMD(year, end_month + 1, 1)
            if end_month < 12
            else ee.Date.fromYMD(year + 1, 1, 1).advance(1, "month")
        )

        full_year_collection = era5_collection.filterDate(
            gap_fill_start, gap_fill_end
        ).select(core_variables)

        # Apply temporal gap filling
        gap_filled_collection = apply_temporal_gap_filling(
            full_year_collection, region_fc.geometry(), core_variables
        )

        # Use gap-filled bands for processing
        core_variables_filled = [v + "_filled" for v in core_variables]
    else:
        gap_filled_collection = None
        core_variables_filled = core_variables

    yearly_results = []

    for season_code, params in seasons.items():
        # --- Handle Date Ranges (especially for winter) ---
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

        # --- Filter and Pre-process Data for the Season ---
        if apply_gap_filling and gap_filled_collection:
            # Use gap-filled data
            seasonal_data = gap_filled_collection.filterDate(
                start_date, end_date
            )
            # Rename filled bands back to original names for processing
            seasonal_data = seasonal_data.map(
                lambda img: img.select(core_variables_filled, core_variables)
            )
        else:
            # Use original data
            seasonal_data = era5_collection.filterDate(
                start_date, end_date
            ).select(core_variables)

        processed_seasonal_data = seasonal_data.map(process_era5_image)

        # --- 1. Calculate Aggregate Statistics (Mean, Median, Sum) ---
        stat_reducer = (
            ee.Reducer.mean()
            .combine(ee.Reducer.median(), "", True)
            .combine(ee.Reducer.sum(), "", True)
        )

        # Select bands for aggregation (core + derived)
        bands_for_stats = core_variables + derived_variables
        stats_image = processed_seasonal_data.select(bands_for_stats).reduce(
            stat_reducer
        )

        # --- 2. Calculate Exposure Bin Counts ---
        def get_bin_counts(image):
            temp = image.select("temperature_c").round()
            wind = image.select("wind_speed").round()

            # Temperature bins → list of Images → collapse to multi-band Image
            temp_bin_images = temp_bins.map(
                lambda t: temp.eq(ee.Number(t)).rename(
                    ee.String("temp_h_").cat(ee.Number(t).int().format())
                )
            )
            temp_bands = ee.ImageCollection.fromImages(
                temp_bin_images
            ).toBands()

            # Wind bins → list of Images → collapse to multi-band Image
            wind_bin_images = wind_bins.map(
                lambda w: wind.eq(ee.Number(w)).rename(
                    ee.String("wind_h_").cat(ee.Number(w).int().format())
                )
            )
            wind_bands = ee.ImageCollection.fromImages(
                wind_bin_images
            ).toBands()

            return temp_bands.addBands(wind_bands)

        # Map over the collection to create binary images for each hour, then sum them up
        binned_images = processed_seasonal_data.map(get_bin_counts)
        bins_image = binned_images.reduce(ee.Reducer.sum())

        # --- 3. Combine Stats and Bins & Rename Bands ---
        final_image = stats_image.addBands(bins_image)

        # Append season code to each band name
        new_band_names = final_image.bandNames().map(
            lambda b: ee.String(b).cat("_").cat(season_code)
        )
        final_image_renamed = final_image.rename(new_band_names)

        yearly_results.append(final_image_renamed)

    # --- Combine all seasonal images for the year ---
    yearly_image = ee.Image.cat(yearly_results)

    # --- Spatially Reduce to get stats for each region ---
    final_fc = yearly_image.reduceRegions(
        collection=region_fc.select([region_id_property]),
        reducer=ee.Reducer.mean(),  # Use mean to aggregate pixel values within each polygon
        scale=scale,
        tileScale=4,  # Use a larger tileScale to avoid memory issues
    )

    # --- Export to Google Drive ---
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
        f"Started seasonal export task for {region_name} {year}. Filename: {output_filename}"
    )


def process_weather_data_batch(
    start_year=1981,
    end_year=2023,
    export_hourly=True,
    export_seasonal=True,
    apply_gap_filling=True,
    scale=9000,
    folder_name="GEE_WEATHER_EXPORTS",
    groups_of_n_months=3,
):
    """
    Batch process weather data for Mongolia and Inner Mongolia, China from start_year to end_year.
    This is the main function that implements the complete workflow as specified in project requirements.

    Parameters:
    - start_year (int): Starting year for processing (default: 1981)
    - end_year (int): Ending year for processing (default: 2023)
    - export_hourly (bool): Whether to export cleaned hourly data
    - export_seasonal (bool): Whether to export seasonal aggregates
    - apply_gap_filling (bool): Whether to apply temporal gap-filling strategy
    - scale (int): Scale in meters for spatial reduction
    - folder_name (str): Google Drive folder for exports
    - groups_of_n_months (int): Number of months to process in each batch (default: 3)
    """

    # Load administrative boundaries
    print("Loading administrative boundaries...")

    # Mongolia: GADM level 2 (Soums)
    try:
        # Try to load from user assets first (more efficient)
        mng_fc = ee.FeatureCollection("users/avraltod/gadm41_MNG_2")
        mng_id_property = "GID_2"
        print("✓ Loaded Mongolia boundaries from user assets")
    except:
        # Fallback: could implement GADM download here if needed
        print(
            "✗ Could not load Mongolia boundaries. Please ensure 'users/avraltod/gadm41_MNG_2' asset exists."
        )
        return

    # Inner Mongolia, China: GADM level 3
    try:
        # Try to load from user assets first (more efficient)
        chn_fc = ee.FeatureCollection("users/avraltod/gadm41_CHN_3").filter(
            ee.Filter.eq("GID_1", "CHN.19_1")  # Inner Mongolia province
        )
        chn_id_property = "GID_3"
        print("✓ Loaded Inner Mongolia boundaries from user assets")
    except:
        print(
            "✗ Could not load Inner Mongolia boundaries. Please ensure 'users/avraltod/gadm41_CHN_3' asset exists."
        )
        return

    # Process each year
    total_years = end_year - start_year + 1
    print(f"\nProcessing {total_years} years ({start_year}-{end_year})...")
    print(f"Gap filling: {'ENABLED' if apply_gap_filling else 'DISABLED'}")
    print(f"Hourly export: {'ENABLED' if export_hourly else 'DISABLED'}")
    print(f"Seasonal export: {'ENABLED' if export_seasonal else 'DISABLED'}")
    print(f"Export folder: {folder_name}\n")

    for year in range(start_year, end_year + 1):
        print(f"{'='*60}")
        print(
            f"PROCESSING YEAR: {year} ({year - start_year + 1}/{total_years})"
        )
        print(f"{'='*60}")

        # Create month batches
        month_batches = []
        for start_month in range(1, 13, groups_of_n_months):
            end_month = min(start_month + groups_of_n_months - 1, 12)
            month_batches.append((start_month, end_month))

        print(
            f"Processing in {len(month_batches)} month batches: {month_batches}"
        )

        for batch_idx, (start_month, end_month) in enumerate(month_batches):
            print(
                f"\n--- BATCH {batch_idx + 1}/{len(month_batches)}: MONTHS {start_month}-{end_month} ---"
            )

            # Process Mongolia
            print(f"\n--- MONGOLIA (GADM Level 2: Soums) ---")
            try:
                if export_seasonal:
                    print("Exporting seasonal aggregates...")
                    export_seasonal_weather_stats(
                        year=year,
                        region_fc=mng_fc,
                        region_name="MNG",
                        region_id_property=mng_id_property,
                        scale=scale,
                        folder_name=folder_name,
                        apply_gap_filling=apply_gap_filling,
                        start_month=start_month,
                        end_month=end_month,
                    )

                if export_hourly:
                    print("Exporting cleaned hourly data...")
                    export_hourly_weather_data(
                        year=year,
                        region_fc=mng_fc,
                        region_name="MNG",
                        region_id_property=mng_id_property,
                        scale=scale,
                        folder_name=folder_name,
                        apply_gap_filling=apply_gap_filling,
                        start_month=start_month,
                        end_month=end_month,
                    )

            except Exception as e:
                print(
                    f"ERROR processing Mongolia for year {year}, months {start_month}-{end_month}: {e}"
                )

            # Process Inner Mongolia, China
            print(f"\n--- INNER MONGOLIA, CHINA (GADM Level 3: Counties) ---")
            try:
                if export_seasonal:
                    print("Exporting seasonal aggregates...")
                    export_seasonal_weather_stats(
                        year=year,
                        region_fc=chn_fc,
                        region_name="CHN_IM",
                        region_id_property=chn_id_property,
                        scale=scale,
                        folder_name=folder_name,
                        apply_gap_filling=apply_gap_filling,
                        start_month=start_month,
                        end_month=end_month,
                    )

                if export_hourly:
                    print("Exporting cleaned hourly data...")
                    export_hourly_weather_data(
                        year=year,
                        region_fc=chn_fc,
                        region_name="CHN_IM",
                        region_id_property=chn_id_property,
                        scale=scale,
                        folder_name=folder_name,
                        apply_gap_filling=apply_gap_filling,
                        start_month=start_month,
                        end_month=end_month,
                    )

            except Exception as e:
                print(
                    f"ERROR processing Inner Mongolia for year {year}, months {start_month}-{end_month}: {e}"
                )

        print(f"\nCompleted year {year}")

    print(f"\n{'='*60}")
    print(f"BATCH PROCESSING COMPLETE")
    print(f"Processed {total_years} years for 2 regions")
    print(f"Check Google Earth Engine Task Manager for export progress")
    print(f"Files will be saved to Google Drive folder: {folder_name}")


def process_single_region_batch(
    region_name,
    region_fc,
    region_id_property,
    start_year=1981,
    end_year=2023,
    export_hourly=True,
    export_seasonal=True,
    apply_gap_filling=True,
    scale=9000,
    folder_name="GEE_WEATHER_EXPORTS",
    groups_of_n_months=3,
):
    """
    Process weather data for a single region across multiple years.
    Useful for processing custom regions or when you want more control.

    Parameters:
    - region_name (str): Name for the region (e.g., 'MNG', 'CHN_IM')
    - region_fc (ee.FeatureCollection): Administrative boundaries
    - region_id_property (str): Property holding unique ID for each feature
    - start_year (int): Starting year for processing
    - end_year (int): Ending year for processing
    - export_hourly (bool): Whether to export cleaned hourly data
    - export_seasonal (bool): Whether to export seasonal aggregates
    - apply_gap_filling (bool): Whether to apply temporal gap-filling strategy
    - scale (int): Scale in meters for spatial reduction
    - folder_name (str): Google Drive folder for exports
    - groups_of_n_months (int): Number of months to process in each batch (default: 3)
    """

    total_years = end_year - start_year + 1
    print(
        f"Processing {region_name}: {total_years} years ({start_year}-{end_year})"
    )

    for year in range(start_year, end_year + 1):
        print(
            f"\nProcessing {region_name} - Year {year} ({year - start_year + 1}/{total_years})"
        )

        # Create month batches
        month_batches = []
        for start_month in range(1, 13, groups_of_n_months):
            end_month = min(start_month + groups_of_n_months - 1, 12)
            month_batches.append((start_month, end_month))

        print(
            f"Processing in {len(month_batches)} month batches: {month_batches}"
        )

        for batch_idx, (start_month, end_month) in enumerate(month_batches):
            print(
                f"  Batch {batch_idx + 1}/{len(month_batches)}: Months {start_month}-{end_month}"
            )

            try:
                if export_seasonal:
                    export_seasonal_weather_stats(
                        year=year,
                        region_fc=region_fc,
                        region_name=region_name,
                        region_id_property=region_id_property,
                        scale=scale,
                        folder_name=folder_name,
                        apply_gap_filling=apply_gap_filling,
                        start_month=start_month,
                        end_month=end_month,
                    )

                if export_hourly:
                    export_hourly_weather_data(
                        year=year,
                        region_fc=region_fc,
                        region_name=region_name,
                        region_id_property=region_id_property,
                        scale=scale,
                        folder_name=folder_name,
                        apply_gap_filling=apply_gap_filling,
                        start_month=start_month,
                        end_month=end_month,
                    )

            except Exception as e:
                print(
                    f"✗ ERROR processing {region_name} for year {year}, months {start_month}-{end_month}: {e}"
                )

    print(f"\n✓ Completed processing {region_name}")


# Example usage and testing functions
def run_sample_processing():
    """
    Run a sample processing for testing - processes just 2 years for both regions.
    Useful for testing the workflow before running the full 1981-2023 batch.
    """
    print("Running sample processing (2022-2023)...")
    process_weather_data_batch(
        start_year=2022,
        end_year=2023,
        export_hourly=True,
        export_seasonal=True,
        apply_gap_filling=True,
        folder_name="GEE_WEATHER_SAMPLE",
        groups_of_n_months=3,  # Process in 3-month batches for testing
    )


def run_full_processing():
    """
    Run the complete processing for 1981-2023 as specified in project requirements.
    This will generate all required outputs for both regions.
    """
    print("Running full processing (1981-2023)...")
    process_weather_data_batch(
        start_year=1981,
        end_year=2023,
        export_hourly=True,
        export_seasonal=True,
        apply_gap_filling=True,
        folder_name="GEE_WEATHER_FINAL",
        groups_of_n_months=3,  # Process in 3-month batches to avoid memory issues
    )
