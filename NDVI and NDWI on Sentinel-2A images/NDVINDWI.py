from inspect import getcomments
from pickle import FALSE, TRUE
import ee
import geemap
import datetime
import pandas as pd
import shapely.wkt
import multiprocessing
ee.Initialize()

visParamsTrue = {'bands': ['B4', 'B3', 'B2'], min: 0, max: 2000}
CLOUD_FILTER = 50
CLD_PRB_THRESH = 50
NIR_DRK_THRESH = 0.15
CLD_PRJ_DIST = 1
BUFFER = 5


def transformPolygon(polygon):
    Polygon = shapely.wkt.loads(polygon)
    input_polygon = shapely.wkt.dumps(Polygon)[10:-2]
    split_polygon = input_polygon.split(",")
    return_arr = []
    for sub_arr in split_polygon:
        split_subarr = sub_arr.split(" ")
        clean_split_subbarr = list(filter(None, split_subarr))
        return_arr.append(clean_split_subbarr)
    return_str = [[float(c) for c in row] for row in return_arr]
    return return_str


def get_s2_sr_cld_col(geom, start_date, end_date):
    s2_sr_col = (ee.ImageCollection('COPERNICUS/S2_SR')
                 .filterBounds(geom)
                 .filterDate(start_date, end_date)
                 .sort('CLOUD_COVERAGE_ASSESSMENT'))
                # .filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', CLOUD_FILTER)))
    s2_cloudless_col = (ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY')
                        .filterBounds(geom)
                        .filterDate(start_date, end_date))
    return ee.ImageCollection(ee.Join.saveFirst('s2cloudless').apply(
        primary=s2_sr_col,
        secondary=s2_cloudless_col,
        condition=ee.Filter.equals(
            leftField='system:index',
            rightField='system:index'
        )
    ))


def add_cloud_bands(img):
    cld_prb = ee.Image(img.get('s2cloudless')).select('probability')
    is_cloud = cld_prb.gt(CLD_PRB_THRESH).rename('clouds')
    return img.addBands(ee.Image([cld_prb, is_cloud]))


def add_shadow_bands(img):
    not_water = img.select('SCL').neq(6)
    SR_BAND_SCALE = 1e4
    dark_pixels = img.select('B8').lt(
        NIR_DRK_THRESH*SR_BAND_SCALE).multiply(not_water).rename('dark_pixels')
    shadow_azimuth = ee.Number(90).subtract(
        ee.Number(img.get('MEAN_SOLAR_AZIMUTH_ANGLE')))
    cld_proj = (img.select('clouds').directionalDistanceTransform(shadow_azimuth, CLD_PRJ_DIST*10)
                # .reproject({'crs': img.select(0).projection(), 'scale': 100})
                .select('distance')
                .mask()
                .rename('cloud_transform'))
    shadows = cld_proj.multiply(dark_pixels).rename('shadows')
    return img.addBands(ee.Image([dark_pixels, cld_proj, shadows]))


def add_cld_shdw_mask(img):
    img_cloud = add_cloud_bands(img)
    img_cloud_shadow = add_shadow_bands(img_cloud)
    is_cld_shdw = img_cloud_shadow.select('clouds').add(
        img_cloud_shadow.select('shadows')).gt(0)
    is_cld_shdw2 = (is_cld_shdw.focal_min(2).focal_max(BUFFER*2/20)
                    # .reproject({'crs': img.select([0]).projection(), 'scale': 20})
                    .rename('cloudmask'))
    return img_cloud_shadow.addBands(is_cld_shdw2)


def apply_cld_shdw_mask(img):
    not_cld_shdw = img.select('cloudmask').Not()
    return img.select('B.*').updateMask(not_cld_shdw)


def calculate_ndvi_ndwi(image_masked, geom):
    ndwi = image_masked.normalizedDifference(['B3', 'B8']).rename('NDWI')
    ndvi = image_masked.normalizedDifference(['B8', 'B4']).rename('NDVI')
    # print('NDWI')
    avg_ndwi_mosaic = ndwi.reduceRegion(
        ee.Reducer.mean(), geom, 10).get('NDWI').getInfo()
    # max = ndwi.reduceRegion(ee.Reducer.max(), geom, 10).get('NDWI').getInfo()
    # min = ndwi.reduceRegion(ee.Reducer.min(), geom, 10).get('NDWI').getInfo()
    # print('avg', avg_ndwi_mosaic, 'min', min, 'max', max)
    ndwiParams = {min: -1, max: 1, 'palette': ['8E3200', '#A64B2A', 'D7A86E',
                                               '#FFEBC1', '#E3CAA5', '#d7efa9', '#9ecae1', '#4292c6', '#2171b5', '#08519c']}
    # if (avg_ndwi_mosaic >= -1 and avg_ndwi_mosaic <= -0.3):
    #     print("NDWI result: Drought")
    # elif(avg_ndwi_mosaic >= -0.3 and avg_ndwi_mosaic <= 0.0):
    #     print("NDWI result: Moderately Drought")
    # elif(avg_ndwi_mosaic >= 0.0 and avg_ndwi_mosaic <= 0.2):
    #     print("NDWI result: Flood")
    # else:
    #     print("NDWI result: Water surface")
    flood_threshold_ndwi = 0
    flood_ndwi = ndwi.gte(flood_threshold_ndwi).selfMask()
    flood_pixel_area_ndwi = flood_ndwi.multiply(ee.Image.pixelArea())
    flood_area_ndwi = flood_pixel_area_ndwi.reduceRegion(**{
        'reducer': ee.Reducer.sum(),
        'geometry': geom,
        'scale': 10
    })
    # print(flood_area_ndwi.getInfo(), 'flood area ndwi')
    drought_threshold1_ndwi = 0
    drought_ndwi = ndwi.lt(drought_threshold1_ndwi).selfMask()
    drought_pixel_area_ndwi = drought_ndwi.multiply(ee.Image.pixelArea())
    drought_area_ndwi = drought_pixel_area_ndwi.reduceRegion(**{
        'reducer': ee.Reducer.sum(),
        'geometry': geom,
        'scale': 10
    })
    # print(drought_area_ndwi.getInfo(), 'drought area ndwi')
    # print('NDVI')
    avg_ndvi_mosaic = ndvi.reduceRegion(
        ee.Reducer.mean(), geom, 10).get('NDVI').getInfo()
    # max = ndvi.reduceRegion(ee.Reducer.max(), geom, 10).get('NDVI').getInfo()
    # min = ndvi.reduceRegion(ee.Reducer.min(), geom, 10).get('NDVI').getInfo()
    # print('avg', avg_ndvi_mosaic, 'min', min, 'max', max)
    ndviParams = {min: -1, max: 1, 'palette': ['#08306b', '#0850d1', '#2171b5',
                                               '#4292c6', '#9ecae1', '#b8d4e1', '#d7a86e', '#e9efc0', '#83bd75', '#4e944f']}
    # if (avg_ndvi_mosaic >= 0.1 and avg_ndvi_mosaic <= 0.4):
    #     print("NDVI result: Drought")
    # elif(avg_ndvi_mosaic > 0.4 and avg_ndvi_mosaic <= 1):
    #     print("NDVI result: Vegetation")
    # elif(avg_ndvi_mosaic >= -1 and avg_ndvi_mosaic < 0.1):
    #     print("NDVI result: Flood")

    flood_threshold_ndvi = 0.17
    flood_ndvi = ndvi.lt(flood_threshold_ndvi).selfMask()
    flood_pixel_area_ndvi = flood_ndvi.multiply(ee.Image.pixelArea())
    flood_area_ndvi = flood_pixel_area_ndvi.reduceRegion(**{
        'reducer': ee.Reducer.sum(),
        'geometry': geom,
        'scale': 10
    })
    # print(flood_area_ndvi.getInfo(), 'flood area ndvi')
    drought_threshold1_ndvi = 0.17
    drought_threshold2_ndvi = 0.45
    drought_ndvi = ndvi.gte(drought_threshold1_ndvi).And(
        ndvi.lte(drought_threshold2_ndvi)).selfMask()
    drought_pixel_area_ndvi = drought_ndvi.multiply(ee.Image.pixelArea())
    drought_area_ndvi = drought_pixel_area_ndvi.reduceRegion(**{
        'reducer': ee.Reducer.sum(),
        'geometry': geom,
        'scale': 10
    })
    # print(drought_area_ndvi.getInfo(), 'drought area ndvi')
    vegetation_threshold1_ndvi = 0.45
    vegetation_ndvi = ndvi.gt(vegetation_threshold1_ndvi).selfMask()
    vegetation_pixel_area_ndvi = vegetation_ndvi.multiply(ee.Image.pixelArea())
    vegetation_area_ndvi = vegetation_pixel_area_ndvi.reduceRegion(**{
        'reducer': ee.Reducer.sum(),
        'geometry': geom,
        'scale': 10
    })
    # print(vegetation_area_ndvi.getInfo(), 'vegetation area ndvi')
    return {'flood_area_ndwi': flood_area_ndwi.getInfo()['NDWI'], 'drought_area_ndwi': drought_area_ndwi.getInfo()['NDWI'], 'avg_ndwi_mosaic':avg_ndwi_mosaic, 'flood_area_ndvi': flood_area_ndvi.getInfo()['NDVI'], 'drought_area_ndvi': drought_area_ndvi.getInfo()['NDVI'], 'vegetation_area_ndvi': vegetation_area_ndvi.getInfo()['NDVI'],'avg_ndvi_mosaic':avg_ndvi_mosaic}


def compute_NDVI_NDWI(geom, input_date):
    try:
        rawPolygon = ee.Geometry.Polygon(transformPolygon(geom))
        START_DATE = datetime.datetime.strptime(input_date, "%Y-%m-%d")
        START_DATE = ee.Date(START_DATE)
        END_DATE = START_DATE.advance(5, 'days')
        s2_sr_cld_col_eval = get_s2_sr_cld_col(
            rawPolygon, START_DATE, END_DATE)
        img = s2_sr_cld_col_eval
        try:
            epochtime = img.aggregate_array('system:time_start').getInfo()[-1]
        except:
            epochtime = 0
        img = s2_sr_cld_col_eval.map(add_cld_shdw_mask)
        img = img.first()
        image_masked = apply_cld_shdw_mask(img)


        area_after_masking = image_masked.reduceRegion(
            ee.Reducer.sum(), rawPolygon, 10).get('B1').getInfo()
        # print(area_after_masking, 'Area after masking within 5 days')

        area_before_masking = img.reduceRegion(
            ee.Reducer.sum(), rawPolygon, 10).get('B1').getInfo()
        # print(area_before_masking, 'Area before masking within 5 days')
        if(area_after_masking == area_before_masking):
            calc_ndvi_ndwi = calculate_ndvi_ndwi(image_masked, rawPolygon)
            return calc_ndvi_ndwi, epochtime
        else:
            return 0, 0
    except:
        return 'error', 0


def compute(dataframe):
    for index, row in dataframe.iterrows():
        print("Working On index: ", index)
        result, epochtime = compute_NDVI_NDWI(
            row['geom'], row['warranty_dmgdate'])
        if(result != 0 and result != 'error'):
            dataframe.at[index, 'flood_area_ndwi'] = str(
                result['flood_area_ndwi'])
            dataframe.at[index, 'drought_area_ndwi'] = str(
                result['drought_area_ndwi'])
            dataframe.at[index, 'avg_ndwi_mosaic'] = str(
                result['avg_ndwi_mosaic'])
            dataframe.at[index, 'flood_area_ndvi'] = str(
                result['flood_area_ndvi'])
            dataframe.at[index, 'drought_area_ndvi'] = str(
                result['drought_area_ndvi'])
            dataframe.at[index, 'vegetation_area_ndvi'] = str(
                result['vegetation_area_ndvi'])
            dataframe.at[index, 'avg_ndvi_mosaic'] = str(
                result['avg_ndvi_mosaic'])
        else:
            dataframe.at[index, 'flood_area_ndwi'] = "-"
            dataframe.at[index, 'drought_area_ndwi'] = "-"
            dataframe.at[index, 'avg_ndwi_mosaic'] = "-"
            dataframe.at[index, 'flood_area_ndvi'] = "-"
            dataframe.at[index, 'drought_area_ndvi'] = "-"
            dataframe.at[index, 'vegetation_area_ndvi'] = "-"
            dataframe.at[index, 'avg_ndvi_mosaic'] = "-"
        if(epochtime != 0 and result != 'error'):
            normalTime = datetime.datetime.fromtimestamp(
                epochtime/1000).strftime("%Y-%m-%d")
            dataframe.at[index, 'sat_time'] = normalTime
        elif(result == 'error'):
            dataframe.at[index, 'sat_time'] = "Calculation Error"
        else:
            dataframe.at[index, 'sat_time'] = "Cloudy"
    return dataframe


# geom = ee.Geometry.Polygon([[102.5633930333, 15.3911126506452], [102.5633412905, 15.3911006621863], [102.5632589747, 15.3910814856279], [102.5630058721, 15.3910283104296], [
#     102.5628613992, 15.3909717388604], [102.5629181022, 15.3884164751931], [102.5631823462, 15.3885207260593], [102.5635976384, 15.3886884700794], [102.5633930333, 15.3911126506452]])
# START_DATE = ee.Date('2021-5-5')


if __name__ == "__main__":
    df = pd.read_csv('./raw_data_800.csv', header=0)
    num_processes = multiprocessing.cpu_count()
    chunk_size = df.shape[0] if(
        int(df.shape[0]/num_processes) == 0) else int(df.shape[0]/num_processes)
    chunks = [df.iloc[df.index[i:i + chunk_size]]
              for i in range(0, df.shape[0], chunk_size)]
    pool = multiprocessing.Pool(processes=num_processes)
    result = pool.map(compute, chunks)
    df2 = pd.DataFrame()
    for i in range(len(result)):
        df2 = pd.concat([df2, result[i]], axis=0)
    df2.to_csv(r'C:\Users\geoma\Downloads\ee\ee\testndvindwi.csv',
               index=False, header=True)
