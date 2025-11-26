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

    # --- Define date range ---
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

    # --- Optional gap filling ---
    if apply_gap_filling:
        gap_fill_start = start_date.advance(-1, "month")
        gap_fill_end = end_date.advance(1, "month")

        gap_fill_collection = (
            ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")
            .filterBounds(region_fc.geometry())
            .filterDate(gap_fill_start, gap_fill_end)
            .select(core_variables)
        )

        gap_filled_collection = apply_temporal_gap_filling(
            gap_fill_collection, region_fc.geometry(), core_variables
        )

        era5_collection = gap_filled_collection.filterDate(start_date, end_date)
        variables_to_process = [v + "_filled" for v in core_variables]
    else:
        variables_to_process = core_variables


    # --- Function to classify month into season ---
    def month_to_season(month):
        """Return season name given ee.Number month"""
        month = ee.Number(month)
        return (
            ee.Algorithms.If(month.eq(12).Or(month.lte(2)), "wtr",
            ee.Algorithms.If(month.gte(3).And(month.lte(5)), "spr",
            ee.Algorithms.If(month.gte(6).And(month.lte(8)), "smr",
            "aut"))))


    # --- Image processing ---
    def process_hourly_image(img):
        if apply_gap_filling:
            processed = img.select(variables_to_process, core_variables)
        else:
            processed = img.select(variables_to_process)

        processed = process_era5_image(processed)

        month = img.date().get("month")
        season = month_to_season(month)

        processed = processed.set({
            "year": img.date().get("year"),
            "month": month,
            "day": img.date().get("day"),
            "hour": img.date().get("hour"),
            "season": season,
            "system:time_start": img.get("system:time_start"),
        })

        return processed


    processed_collection = era5_collection.map(process_hourly_image)


    # --- Reduction step ---
    def reduce_to_regions(img):
        reduced = img.reduceRegions(
            collection=region_fc.select([region_id_property]),
            reducer=ee.Reducer.mean(),
            scale=scale,
            tileScale=4,
        )

        def add_time_props(feature):
            return feature.set({
                "year": img.get("year"),
                "month": img.get("month"),
                "day": img.get("day"),
                "hour": img.get("hour"),
                "season": img.get("season"),
                "system:time_start": img.get("system:time_start"),
            })

        return reduced.map(add_time_props)


    hourly_fc = processed_collection.map(reduce_to_regions).flatten()

    # Set geometry to null for all features
    hourly_fc = hourly_fc.map(lambda f: f.setGeometry(None))

    # --- Export task ---
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
    print(f"Started hourly export task for {region_name} {year}. Filename: {output_filename}")



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
