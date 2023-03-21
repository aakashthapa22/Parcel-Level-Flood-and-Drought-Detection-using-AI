from inspect import getcomments
import ee
import geemap
import datetime
import pandas as pd
import shapely.wkt
import multiprocessing
ee.Initialize()


def calculate_drought(point, disaster_date):
    dry_soil = ee.FeatureCollection([ee.Feature(
        ee.Geometry.Point([102.13134324149291, 14.908817540274544]),
        {
            "LC": 0,
            "system:index": "0"
        }),
        ee.Feature(
        ee.Geometry.Point([102.12301766471069, 14.908195479424588]),
        {
            "LC": 0,
            "system:index": "1"
        }),
        ee.Feature(
        ee.Geometry.Point([102.1240476329724, 14.905956045477279]),
        {
            "LC": 0,
            "system:index": "2"
        }),
        ee.Feature(
        ee.Geometry.Point([102.1257213313977, 14.908983422864186]),
        {
            "LC": 0,
            "system:index": "3"
        }),
        ee.Feature(
        ee.Geometry.Point([102.13091408805053, 14.90496073402156]),
        {
            "LC": 0,
            "system:index": "4"
        }),
        ee.Feature(
        ee.Geometry.Point([102.13092830390954, 14.904905654437401]),
        {
            "LC": 0,
            "system:index": "5"
        }),
        ee.Feature(
        ee.Geometry.Point([102.13815665718221, 14.906548756521282]),
        {
            "LC": 0,
            "system:index": "6"
        }),
        ee.Feature(
        ee.Geometry.Point([102.13723617728998, 14.912271692304847]),
        {
            "LC": 0,
            "system:index": "7"
        }),
        ee.Feature(
        ee.Geometry.Point([102.13835197624017, 14.915298980835567]),
        {
            "LC": 0,
            "system:index": "8"
        }),
        ee.Feature(
        ee.Geometry.Point([102.13448959525873, 14.917123627316904]),
        {
            "LC": 0,
            "system:index": "9"
        }),
        ee.Feature(
        ee.Geometry.Point([102.13076882160455, 14.914279050799323]),
        {
            "LC": 0,
            "system:index": "10"
        }),
        ee.Feature(
        ee.Geometry.Point([102.12961010731011, 14.915689012943295]),
        {
            "LC": 0,
            "system:index": "11"
        }),
        ee.Feature(
        ee.Geometry.Point([102.13051132953912, 14.920084717947615]),
        {
            "LC": 0,
            "system:index": "12"
        }),
        ee.Feature(
        ee.Geometry.Point([102.1321081920013, 14.896559871611146]),
        {
            "LC": 0,
            "system:index": "13"
        }),
        ee.Feature(
        ee.Geometry.Point([102.13348148301692, 14.895357150540171]),
        {
            "LC": 0,
            "system:index": "14"
        }),
        ee.Feature(
        ee.Geometry.Point([102.09755268839056, 14.889637582372048]),
        {
            "LC": 0,
            "system:index": "15"
        }),
        ee.Feature(
        ee.Geometry.Point([102.28767267176237, 14.903530098688137]),
        {
            "LC": 0,
            "system:index": "16"
        }),
        ee.Feature(
        ee.Geometry.Point([102.28200784632291, 14.90460835972355]),
        {
            "LC": 0,
            "system:index": "17"
        }),
        ee.Feature(
        ee.Geometry.Point([102.26788869806852, 14.903364211895635]),
        {
            "LC": 0,
            "system:index": "18"
        }),
        ee.Feature(
        ee.Geometry.Point([102.278960856882, 14.903695985352806]),
        {
            "LC": 0,
            "system:index": "19"
        }),
        ee.Feature(
        ee.Geometry.Point([102.26947656580533, 14.89971467012332]),
        {
            "LC": 0,
            "system:index": "20"
        }),
        ee.Feature(
        ee.Geometry.Point([102.27921555671007, 14.898686721854602]),
        {
            "LC": 0,
            "system:index": "21"
        }),
        ee.Feature(
        ee.Geometry.Point([102.10117292757128, 14.905409934481485]),
        {
            "LC": 0,
            "system:index": "22"
        }),
        ee.Feature(
        ee.Geometry.Point([102.15919447298144, 14.897281402315805]),
        {
            "LC": 0,
            "system:index": "23"
        }),
        ee.Feature(
        ee.Geometry.Point([102.17612879029969, 14.899910842126738]),
        {
            "LC": 0,
            "system:index": "24"
        }),
        ee.Feature(
        ee.Geometry.Point([102.18325273744324, 14.903933625337144]),
        {
            "LC": 0,
            "system:index": "25"
        }),
        ee.Feature(
        ee.Geometry.Point([102.1762022915469, 14.882390480846512]),
        {
            "LC": 0,
            "system:index": "26"
        }),
        ee.Feature(
        ee.Geometry.Point([102.17748975187405, 14.881270632643114]),
        {
            "LC": 0,
            "system:index": "27"
        }),
        ee.Feature(
        ee.Geometry.Point([102.1796784344302, 14.880399635574953]),
        {
            "LC": 0,
            "system:index": "28"
        }),
        ee.Feature(
        ee.Geometry.Point([102.18096589475735, 14.880358159436327]),
        {
            "LC": 0,
            "system:index": "29"
        }),
        ee.Feature(
        ee.Geometry.Point([102.16521787011278, 14.877777232162392]),
        {
            "LC": 0,
            "system:index": "30"
        }),
        ee.Feature(
        ee.Geometry.Point([102.16744946801317, 14.871970424540178]),
        {
            "LC": 0,
            "system:index": "31"
        }),
        ee.Feature(
        ee.Geometry.Point([100.44806876441774, 15.856362493488156]),
        {
            "LC": 0,
            "system:index": "32"
        }),
        ee.Feature(
        ee.Geometry.Point([100.44489302894411, 15.859169677275434]),
        {
            "LC": 0,
            "system:index": "33"
        }),
        ee.Feature(
        ee.Geometry.Point([100.46492895740056, 15.860420113314422]),
        {
            "LC": 0,
            "system:index": "34"
        }),
        ee.Feature(
        ee.Geometry.Point([100.45867738925138, 15.854550336176723]),
        {
            "LC": 0,
            "system:index": "35"
        }),
        ee.Feature(
        ee.Geometry.Point([100.45884905062833, 15.8563048463836]),
        {
            "LC": 0,
            "system:index": "36"
        }),
        ee.Feature(
        ee.Geometry.Point([100.46039400302091, 15.85638741119386]),
        {
            "LC": 0,
            "system:index": "37"
        }),
        ee.Feature(
        ee.Geometry.Point([100.47397510886675, 15.82031143989821]),
        {
            "LC": 0,
            "system:index": "38"
        }),
        ee.Feature(
        ee.Geometry.Point([100.48000471473223, 15.819134677986979]),
        {
            "LC": 0,
            "system:index": "39"
        }),
        ee.Feature(
        ee.Geometry.Point([100.47893183112627, 15.818515326862096]),
        {
            "LC": 0,
            "system:index": "40"
        }),
        ee.Feature(
        ee.Geometry.Point([100.47725813270098, 15.819485642782398]),
        {
            "LC": 0,
            "system:index": "41"
        }),
        ee.Feature(
        ee.Geometry.Point([100.47519819617754, 15.82159141876276]),
        {
            "LC": 0,
            "system:index": "42"
        })])
    green_paddy = ee.FeatureCollection([ee.Feature(
        ee.Geometry.Point([102.28775571021349, 14.901880089533524]),
        {
            "LC": 1,
            "system:index": "0"
        }),
        ee.Feature(
        ee.Geometry.Point([102.29166100653917, 14.905031938460786]),
        {
            "LC": 1,
            "system:index": "1"
        }),
        ee.Feature(
        ee.Geometry.Point([102.29294846686632, 14.899516172557798]),
        {
            "LC": 1,
            "system:index": "2"
        }),
        ee.Feature(
        ee.Geometry.Point([102.30080197486193, 14.898520831330599]),
        {
            "LC": 1,
            "system:index": "3"
        }),
        ee.Feature(
        ee.Geometry.Point([102.30196068915636, 14.90163125738981]),
        {
            "LC": 1,
            "system:index": "4"
        }),
        ee.Feature(
        ee.Geometry.Point([102.2983128848961, 14.903870736329983]),
        {
            "LC": 1,
            "system:index": "5"
        }),
        ee.Feature(
        ee.Geometry.Point([102.29492257270128, 14.9031242459383]),
        {
            "LC": 1,
            "system:index": "6"
        }),
        ee.Feature(
        ee.Geometry.Point([102.29470799598009, 14.896944864979305]),
        {
            "LC": 1,
            "system:index": "7"
        }),
        ee.Feature(
        ee.Geometry.Point([102.2737601322807, 14.900428564641242]),
        {
            "LC": 1,
            "system:index": "8"
        }),
        ee.Feature(
        ee.Geometry.Point([102.27144270369183, 14.90507340984962]),
        {
            "LC": 1,
            "system:index": "9"
        }),
        ee.Feature(
        ee.Geometry.Point([102.27092771956097, 14.910879325415198]),
        {
            "LC": 1,
            "system:index": "10"
        }),
        ee.Feature(
        ee.Geometry.Point([102.29820644352455, 14.89146871619713]),
        {
            "LC": 1,
            "system:index": "11"
        }),
        ee.Feature(
        ee.Geometry.Point([102.30009471867103, 14.888067820970619]),
        {
            "LC": 1,
            "system:index": "12"
        }),
        ee.Feature(
        ee.Geometry.Point([102.30859195683021, 14.893293564677606]),
        {
            "LC": 1,
            "system:index": "13"
        }),
        ee.Feature(
        ee.Geometry.Point([102.3082486340763, 14.897772672683137]),
        {
            "LC": 1,
            "system:index": "14"
        }),
        ee.Feature(
        ee.Geometry.Point([102.30249797794838, 14.896777323397902]),
        {
            "LC": 1,
            "system:index": "15"
        }),
        ee.Feature(
        ee.Geometry.Point([102.29247790357026, 14.889352806968308]),
        {
            "LC": 1,
            "system:index": "16"
        }),
        ee.Feature(
        ee.Geometry.Point([102.28492480298432, 14.888855113449601]),
        {
            "LC": 1,
            "system:index": "17"
        }),
        ee.Feature(
        ee.Geometry.Point([102.17273847810486, 14.903228606907751]),
        {
            "LC": 1,
            "system:index": "18"
        }),
        ee.Feature(
        ee.Geometry.Point([102.17921869508484, 14.899662007707287]),
        {
            "LC": 1,
            "system:index": "19"
        }),
        ee.Feature(
        ee.Geometry.Point([100.67045516312918, 15.871330897390422]),
        {
            "LC": 1,
            "system:index": "20"
        }),
        ee.Feature(
        ee.Geometry.Point([100.67019767106375, 15.874633217297218]),
        {
            "LC": 1,
            "system:index": "21"
        }),
        ee.Feature(
        ee.Geometry.Point([100.67165679276785, 15.867037800609817]),
        {
            "LC": 1,
            "system:index": "22"
        }),
        ee.Feature(
        ee.Geometry.Point([102.15325592188205, 14.881879478560547]),
        {
            "LC": 1,
            "system:index": "23"
        }),
        ee.Feature(
        ee.Geometry.Point([102.15548751978244, 14.889510899873368]),
        {
            "LC": 1,
            "system:index": "24"
        }),
        ee.Feature(
        ee.Geometry.Point([102.1530842605051, 14.881049959965349]),
        {
            "LC": 1,
            "system:index": "25"
        }),
        ee.Feature(
        ee.Geometry.Point([102.16912862158108, 14.89547796770499]),
        {
            "LC": 1,
            "system:index": "26"
        }),
        ee.Feature(
        ee.Geometry.Point([102.17196103430081, 14.894565554648711]),
        {
            "LC": 1,
            "system:index": "27"
        })])
    water = ee.FeatureCollection([ee.Feature(
        ee.Geometry.Point([102.28801470776948, 14.893914943991247]),
        {
            "LC": 2,
            "system:index": "0"
        }),
        ee.Feature(
        ee.Geometry.Point([102.22656084962941, 14.883754130676792]),
        {
            "LC": 2,
            "system:index": "1"
        }),
        ee.Feature(
        ee.Geometry.Point([102.21048771551666, 14.897451622130756]),
        {
            "LC": 2,
            "system:index": "2"
        }),
        ee.Feature(
        ee.Geometry.Point([102.2096294086319, 14.902262419327943]),
        {
            "LC": 2,
            "system:index": "3"
        }),
        ee.Feature(
        ee.Geometry.Point([102.24748074225006, 14.907073109031671]),
        {
            "LC": 2,
            "system:index": "4"
        }),
        ee.Feature(
        ee.Geometry.Point([102.20422207525787, 14.886336610825774]),
        {
            "LC": 2,
            "system:index": "5"
        }),
        ee.Feature(
        ee.Geometry.Point([102.16577362771149, 14.88318448835309]),
        {
            "LC": 2,
            "system:index": "6"
        }),
        ee.Feature(
        ee.Geometry.Point([102.13547539467926, 14.88841035047158]),
        {
            "LC": 2,
            "system:index": "7"
        }),
        ee.Feature(
        ee.Geometry.Point([102.1373736112728, 14.889304762185196]),
        {
            "LC": 2,
            "system:index": "8"
        }),
        ee.Feature(
        ee.Geometry.Point([102.13711611920738, 14.884161872589903]),
        {
            "LC": 2,
            "system:index": "9"
        }),
        ee.Feature(
        ee.Geometry.Point([102.1948801725521, 14.85910926959606]),
        {
            "LC": 2,
            "system:index": "10"
        }),
        ee.Feature(
        ee.Geometry.Point([102.15014082460304, 14.853590402852493]),
        {
            "LC": 2,
            "system:index": "11"
        }),
        ee.Feature(
        ee.Geometry.Point([102.12178051703039, 14.875059932385529]),
        {
            "LC": 2,
            "system:index": "12"
        })])
    urban = ee.FeatureCollection([ee.Feature(
        ee.Geometry.Point([102.14396806016596, 14.920032951448022]),
        {
            "LC": 3,
            "system:index": "0"
        }),
        ee.Feature(
        ee.Geometry.Point([102.14405389085444, 14.918042454173024]),
        {
            "LC": 3,
            "system:index": "1"
        }),
        ee.Feature(
        ee.Geometry.Point([102.1705755735937, 14.920447636061205]),
        {
            "LC": 3,
            "system:index": "2"
        }),
        ee.Feature(
        ee.Geometry.Point([102.17572541490229, 14.92119406634959]),
        {
            "LC": 3,
            "system:index": "3"
        }),
        ee.Feature(
        ee.Geometry.Point([102.17709870591791, 14.928741160353436]),
        {
            "LC": 3,
            "system:index": "4"
        }),
        ee.Feature(
        ee.Geometry.Point([102.1628508116308, 14.924594438204238]),
        {
            "LC": 3,
            "system:index": "5"
        }),
        ee.Feature(
        ee.Geometry.Point([102.16164918199213, 14.923350405959768]),
        {
            "LC": 3,
            "system:index": "6"
        }),
        ee.Feature(
        ee.Geometry.Point([102.14207978501948, 14.920281762311904]),
        {
            "LC": 3,
            "system:index": "7"
        }),
        ee.Feature(
        ee.Geometry.Point([102.14070649400385, 14.925340854095221]),
        {
            "LC": 3,
            "system:index": "8"
        }),
        ee.Feature(
        ee.Geometry.Point([102.12823038671334, 14.919008081095122]),
        {
            "LC": 3,
            "system:index": "9"
        }),
        ee.Feature(
        ee.Geometry.Point([102.0707991830263, 14.913519177115532]),
        {
            "LC": 3,
            "system:index": "10"
        }),
        ee.Feature(
        ee.Geometry.Point([102.06350357450579, 14.913104479143716]),
        {
            "LC": 3,
            "system:index": "11"
        }),
        ee.Feature(
        ee.Geometry.Point([102.06702263273333, 14.906966855708138]),
        {
            "LC": 3,
            "system:index": "12"
        }),
        ee.Feature(
        ee.Geometry.Point([102.07139999784563, 14.906220376053433]),
        {
            "LC": 3,
            "system:index": "13"
        }),
        ee.Feature(
        ee.Geometry.Point([102.06770927824114, 14.9170026085166]),
        {
            "LC": 3,
            "system:index": "14"
        }),
        ee.Feature(
        ee.Geometry.Point([102.06916839994524, 14.910450393195601]),
        {
            "LC": 3,
            "system:index": "15"
        }),
        ee.Feature(
        ee.Geometry.Point([102.05141470467517, 14.944210439293522]),
        {
            "LC": 3,
            "system:index": "16"
        }),
        ee.Feature(
        ee.Geometry.Point([102.05519125496814, 14.944873859628455]),
        {
            "LC": 3,
            "system:index": "17"
        }),
        ee.Feature(
        ee.Geometry.Point([102.06077024971911, 14.943132376876434]),
        {
            "LC": 3,
            "system:index": "18"
        }),
        ee.Feature(
        ee.Geometry.Point([102.0611994031615, 14.950512849683378]),
        {
            "LC": 3,
            "system:index": "19"
        }),
        ee.Feature(
        ee.Geometry.Point([102.05004141365954, 14.951176250536472]),
        {
            "LC": 3,
            "system:index": "20"
        }),
        ee.Feature(
        ee.Geometry.Point([102.046522355432, 14.95018114848767]),
        {
            "LC": 3,
            "system:index": "21"
        }),
        ee.Feature(
        ee.Geometry.Point([102.0476381543822, 14.943132376876434]),
        {
            "LC": 3,
            "system:index": "22"
        }),
        ee.Feature(
        ee.Geometry.Point([102.05081388985583, 14.938488353810628]),
        {
            "LC": 3,
            "system:index": "23"
        }),
        ee.Feature(
        ee.Geometry.Point([102.05690786873767, 14.943049448773957]),
        {
            "LC": 3,
            "system:index": "24"
        }),
        ee.Feature(
        ee.Geometry.Point([100.47004995547452, 15.844889936147112]),
        {
            "LC": 3,
            "system:index": "25"
        }),
        ee.Feature(
        ee.Geometry.Point([100.46936330996671, 15.841587129225603]),
        {
            "LC": 3,
            "system:index": "26"
        }),
        ee.Feature(
        ee.Geometry.Point([100.47099409304776, 15.842660547401453]),
        {
            "LC": 3,
            "system:index": "27"
        }),
        ee.Feature(
        ee.Geometry.Point([100.46799001895108, 15.8441468093006]),
        {
            "LC": 3,
            "system:index": "28"
        }),
        ee.Feature(
        ee.Geometry.Point([100.47039327822843, 15.839440275755026]),
        {
            "LC": 3,
            "system:index": "29"
        }),
        ee.Feature(
        ee.Geometry.Point([100.57745298999333, 15.865291279888847]),
        {
            "LC": 3,
            "system:index": "30"
        }),
        ee.Feature(
        ee.Geometry.Point([100.58002791064763, 15.86776809911318]),
        {
            "LC": 3,
            "system:index": "31"
        }),
        ee.Feature(
        ee.Geometry.Point([100.57951292651677, 15.86413542050347]),
        {
            "LC": 3,
            "system:index": "32"
        })])
    big_point = ee.Geometry.Polygon([[[96.53561136567768, 15.92775040323366],
                                    [95.0216133877193, 14.713724179213285],
                                    [97.3639749910271, 10.083773917683793],
                                    [99.1217874910271, 6.081814920735302],
                                    [102.5495218660271, 6.169203728579637],
                                    [100.17892607032192, 10.39400208229095],
                                    [100.61837919532192, 12.761633170844641],
                                    [101.14572294532192, 12.246799876769582],
                                    [102.90353544532192, 11.68792970142793],
                                    [103.07931669532192, 12.031986782916377],
                                    [102.72775419532192, 13.189863842175345],
                                    [103.25509794532192, 14.086684178510572],
                                    [105.84787138282192, 14.38485468286176],
                                    [105.18869169532192, 16.671109811368698],
                                    [104.70529325782192, 17.92979434657539],
                                    [101.27755888282192, 18.05518308345687],
                                    [101.49728544532192, 20.13142998528463],
                                    [99.38791044532192, 20.708001149199635],
                                    [97.34445341407192, 18.514172953940854]]])
    image = ee.ImageCollection("COPERNICUS/S1_GRD").filter(ee.Filter.eq("instrumentMode", "IW")).filter(ee.Filter.eq(
        "orbitProperties_pass", "ASCENDING")).filterMetadata("resolution_meters", "equals", 10).filterBounds(big_point).select("VV", "VH")

    classification_image_date = image.filterDate(
        "2020-8-1", "2020-8-15").mosaic()
    SMOOTHING_RADIUS = 10
    filtered_classification_date = classification_image_date.focal_mean(
        SMOOTHING_RADIUS,
        "circle",
        "meters"
    )
    disaster_date = image.filterDate(
        disaster_date[0], disaster_date[1]).mosaic()

    filtered_disaster_date = disaster_date.focal_mean(
        SMOOTHING_RADIUS,
        "circle",
        "meters"
    )
    newfc = dry_soil.merge(green_paddy).merge(urban)
    bands = ["VV", "VH"]
    training = filtered_classification_date.select(bands).sampleRegions(**{
        'collection': newfc,
        'properties': ["LC"],
        'scale': 10,
    })
    classifier = ee.Classifier.smileCart().train(**{
        'features': training,
        'classProperty': "LC",
        'inputProperties': bands,
    })
    classified = filtered_disaster_date.select(bands).classify(classifier)
    total_area = ee.Image.pixelArea().addBands(classified.clip(point)).reduceRegion(**{
        'reducer': ee.Reducer.sum().group(**{'groupField': 1}),
        'geometry': classified.clip(point).geometry(),
        'scale': 10,
        'bestEffort': True,
    })
    return total_area.getInfo()


def calculate_flood(point, disaster_date):
    big_point = ee.Geometry.Polygon([[[96.53561136567768, 15.92775040323366],
                                    [95.0216133877193, 14.713724179213285],
                                    [97.3639749910271, 10.083773917683793],
                                    [99.1217874910271, 6.081814920735302],
                                    [102.5495218660271, 6.169203728579637],
                                    [100.17892607032192, 10.39400208229095],
                                    [100.61837919532192, 12.761633170844641],
                                    [101.14572294532192, 12.246799876769582],
                                    [102.90353544532192, 11.68792970142793],
                                    [103.07931669532192, 12.031986782916377],
                                    [102.72775419532192, 13.189863842175345],
                                    [103.25509794532192, 14.086684178510572],
                                    [105.84787138282192, 14.38485468286176],
                                    [105.18869169532192, 16.671109811368698],
                                    [104.70529325782192, 17.92979434657539],
                                    [101.27755888282192, 18.05518308345687],
                                    [101.49728544532192, 20.13142998528463],
                                    [99.38791044532192, 20.708001149199635],
                                    [97.34445341407192, 18.514172953940854]]])
    non_flood = ee.FeatureCollection([ee.Feature(
        ee.Geometry.Point([102.33452620465962, 15.272127164683617]),
        {
            "LC": 1,
            "system:index": "0"
        }),
        ee.Feature(
        ee.Geometry.Point([102.33470859487264, 15.272541162451894]),
        {
            "LC": 1,
            "system:index": "1"
        }),
        ee.Feature(
        ee.Geometry.Point([102.33533086736409, 15.273938398889396]),
        {
            "LC": 1,
            "system:index": "2"
        }),
        ee.Feature(
        ee.Geometry.Point([102.33454766233174, 15.273896999425013]),
        {
            "LC": 1,
            "system:index": "3"
        }),
        ee.Feature(
        ee.Geometry.Point([102.33554544408528, 15.274580089542814]),
        {
            "LC": 1,
            "system:index": "4"
        }),
        ee.Feature(
        ee.Geometry.Point([102.33305635411946, 15.27416609579752]),
        {
            "LC": 1,
            "system:index": "5"
        }),
        ee.Feature(
        ee.Geometry.Point([102.33271303136556, 15.274279944158907]),
        {
            "LC": 1,
            "system:index": "6"
        }),
        ee.Feature(
        ee.Geometry.Point([102.33706893880574, 15.271071466677537]),
        {
            "LC": 1,
            "system:index": "7"
        }),
        ee.Feature(
        ee.Geometry.Point([102.34567972721393, 15.270827375602]),
        {
            "LC": 1,
            "system:index": "8"
        }),
        ee.Feature(
        ee.Geometry.Point([102.34616252483661, 15.270309874037714]),
        {
            "LC": 1,
            "system:index": "9"
        }),
        ee.Feature(
        ee.Geometry.Point([102.34650584759052, 15.269316267457178]),
        {
            "LC": 1,
            "system:index": "10"
        }),
        ee.Feature(
        ee.Geometry.Point([102.34308334888752, 15.269357667825284]),
        {
            "LC": 1,
            "system:index": "11"
        }),
        ee.Feature(
        ee.Geometry.Point([102.34400602878864, 15.269388718096007]),
        {
            "LC": 1,
            "system:index": "12"
        }),
        ee.Feature(
        ee.Geometry.Point([102.34439226688679, 15.269626770018919]),
        {
            "LC": 1,
            "system:index": "13"
        }),
        ee.Feature(
        ee.Geometry.Point([102.35034677089985, 15.270692825318042]),
        {
            "LC": 1,
            "system:index": "14"
        }),
        ee.Feature(
        ee.Geometry.Point([102.34998199047382, 15.271396625848766]),
        {
            "LC": 1,
            "system:index": "15"
        }),
        ee.Feature(
        ee.Geometry.Point([102.34907003940876, 15.271500125727728]),
        {
            "LC": 1,
            "system:index": "16"
        }),
        ee.Feature(
        ee.Geometry.Point([102.34886619152363, 15.272555821577278]),
        {
            "LC": 1,
            "system:index": "17"
        }),
        ee.Feature(
        ee.Geometry.Point([102.35336597882524, 15.26886696348501]),
        {
            "LC": 1,
            "system:index": "18"
        }),
        ee.Feature(
        ee.Geometry.Point([102.3538809629561, 15.268277006089246]),
        {
            "LC": 1,
            "system:index": "19"
        }),
        ee.Feature(
        ee.Geometry.Point([102.35455687962785, 15.269415518873979]),
        {
            "LC": 1,
            "system:index": "20"
        }),
        ee.Feature(
        ee.Geometry.Point([102.35897104125175, 15.272541014284151]),
        {
            "LC": 1,
            "system:index": "21"
        }),
        ee.Feature(
        ee.Geometry.Point([102.4666959651653, 15.32046322296794]),
        {
            "LC": 1,
            "system:index": "22"
        }),
        ee.Feature(
        ee.Geometry.Point([102.4676096164732, 15.320591381380245]),
        {
            "LC": 1,
            "system:index": "23"
        }),
        ee.Feature(
        ee.Geometry.Point([102.46822652454662, 15.319877398500829]),
        {
            "LC": 1,
            "system:index": "24"
        }),
        ee.Feature(
        ee.Geometry.Point([102.46835527057934, 15.318956460462763]),
        {
            "LC": 1,
            "system:index": "25"
        }),
        ee.Feature(
        ee.Geometry.Point([102.32145980649497, 15.330131063237186]),
        {
            "LC": 1,
            "system:index": "26"
        }),
        ee.Feature(
        ee.Geometry.Point([102.32188895993735, 15.329241211760227]),
        {
            "LC": 1,
            "system:index": "27"
        }),
        ee.Feature(
        ee.Geometry.Point([102.32302621655967, 15.32957231972909]),
        {
            "LC": 1,
            "system:index": "28"
        }),
        ee.Feature(
        ee.Geometry.Point([102.32071645131496, 15.32760783484243]),
        {
            "LC": 1,
            "system:index": "29"
        }),
        ee.Feature(
        ee.Geometry.Point([102.32155330052761, 15.327690612530649]),
        {
            "LC": 1,
            "system:index": "30"
        }),
        ee.Feature(
        ee.Geometry.Point([102.31567323642155, 15.327504043485831]),
        {
            "LC": 1,
            "system:index": "31"
        }),
        ee.Feature(
        ee.Geometry.Point([102.31597364383121, 15.328331819303996]),
        {
            "LC": 1,
            "system:index": "32"
        }),
        ee.Feature(
        ee.Geometry.Point([102.32146680789371, 15.326986681934946]),
        {
            "LC": 1,
            "system:index": "33"
        }),
        ee.Feature(
        ee.Geometry.Point([102.29544162527117, 15.355725277467688]),
        {
            "LC": 1,
            "system:index": "34"
        }),
        ee.Feature(
        ee.Geometry.Point([102.29460477605852, 15.355476977638311]),
        {
            "LC": 1,
            "system:index": "35"
        }),
        ee.Feature(
        ee.Geometry.Point([102.29496955648455, 15.349078902608333]),
        {
            "LC": 1,
            "system:index": "36"
        }),
        ee.Feature(
        ee.Geometry.Point([102.29567765966448, 15.345064557977135]),
        {
            "LC": 1,
            "system:index": "37"
        }),
        ee.Feature(
        ee.Geometry.Point([102.29898214117082, 15.346409578952372]),
        {
            "LC": 1,
            "system:index": "38"
        }),
        ee.Feature(
        ee.Geometry.Point([102.27870464101824, 15.347795976119098]),
        {
            "LC": 1,
            "system:index": "39"
        }),
        ee.Feature(
        ee.Geometry.Point([102.33249528130902, 15.281771938179915]),
        {
            "LC": 1,
            "system:index": "40"
        }),
        ee.Feature(
        ee.Geometry.Point([102.33507020196332, 15.284090208561876]),
        {
            "LC": 1,
            "system:index": "41"
        }),
        ee.Feature(
        ee.Geometry.Point([102.46637569174668, 15.229884587051867]),
        {
            "LC": 1,
            "system:index": "42"
        }),
        ee.Feature(
        ee.Geometry.Point([102.49471196022668, 15.217035956492282]),
        {
            "LC": 1,
            "system:index": "43"
        }),
        ee.Feature(
        ee.Geometry.Point([102.49642857399621, 15.220928523280174]),
        {
            "LC": 1,
            "system:index": "44"
        }),
        ee.Feature(
        ee.Geometry.Point([102.53427990761438, 15.209664728696085]),
        {
            "LC": 1,
            "system:index": "45"
        }),
        ee.Feature(
        ee.Geometry.Point([102.54191883888879, 15.21115555961578]),
        {
            "LC": 1,
            "system:index": "46"
        }),
        ee.Feature(
        ee.Geometry.Point([102.5721310625986, 15.224115233732025]),
        {
            "LC": 1,
            "system:index": "47"
        }),
        ee.Feature(
        ee.Geometry.Point([102.57607927426852, 15.228090486651785]),
        {
            "LC": 1,
            "system:index": "48"
        })])
    water_features = ee.FeatureCollection([ee.Feature(
        ee.Geometry.Point([102.4491370538654, 15.329479105297468]),
        {
            "LC": 0,
            "system:index": "0"
        }),
        ee.Feature(
        ee.Geometry.Point([102.4536217073383, 15.327057865090012]),
        {
            "LC": 0,
            "system:index": "1"
        }),
        ee.Feature(
        ee.Geometry.Point([102.44978078402897, 15.328775499922884]),
        {
            "LC": 0,
            "system:index": "2"
        }),
        ee.Feature(
        ee.Geometry.Point([102.44730294022703, 15.331478821206032]),
        {
            "LC": 0,
            "system:index": "3"
        }),
        ee.Feature(
        ee.Geometry.Point([102.4411016729846, 15.331147716257473]),
        {
            "LC": 0,
            "system:index": "4"
        }),
        ee.Feature(
        ee.Geometry.Point([102.44247496400023, 15.328643718100755]),
        {
            "LC": 0,
            "system:index": "5"
        }),
        ee.Feature(
        ee.Geometry.Point([102.44241042113521, 15.327909626757958]),
        {
            "LC": 0,
            "system:index": "6"
        }),
        ee.Feature(
        ee.Geometry.Point([102.44188129435113, 15.329762211684693]),
        {
            "LC": 0,
            "system:index": "7"
        }),
        ee.Feature(
        ee.Geometry.Point([102.44448629450133, 15.328879256068477]),
        {
            "LC": 0,
            "system:index": "8"
        }),
        ee.Feature(
        ee.Geometry.Point([102.43823999345126, 15.325417905054463]),
        {
            "LC": 0,
            "system:index": "9"
        }),
        ee.Feature(
        ee.Geometry.Point([102.43787521302524, 15.325852492104374]),
        {
            "LC": 0,
            "system:index": "10"
        }),
        ee.Feature(
        ee.Geometry.Point([102.43569926134523, 15.319498992742146]),
        {
            "LC": 0,
            "system:index": "11"
        }),
        ee.Feature(
        ee.Geometry.Point([102.43524865023073, 15.317864063285903]),
        {
            "LC": 0,
            "system:index": "12"
        }),
        ee.Feature(
        ee.Geometry.Point([102.44829491487917, 15.322127270271453]),
        {
            "LC": 0,
            "system:index": "13"
        }),
        ee.Feature(
        ee.Geometry.Point([102.44842366091189, 15.321320164938022]),
        {
            "LC": 0,
            "system:index": "14"
        }),
        ee.Feature(
        ee.Geometry.Point([102.44913176409182, 15.321485725260366]),
        {
            "LC": 0,
            "system:index": "15"
        }),
        ee.Feature(
        ee.Geometry.Point([102.4420775318219, 15.316456684963859]),
        {
            "LC": 0,
            "system:index": "16"
        }),
        ee.Feature(
        ee.Geometry.Point([102.44289292336242, 15.317429372597681]),
        {
            "LC": 0,
            "system:index": "17"
        }),
        ee.Feature(
        ee.Geometry.Point([102.44478992260495, 15.316890950591162]),
        {
            "LC": 0,
            "system:index": "18"
        }),
        ee.Feature(
        ee.Geometry.Point([102.45015151905797, 15.304368671621916]),
        {
            "LC": 0,
            "system:index": "19"
        }),
        ee.Feature(
        ee.Geometry.Point([102.44991548466466, 15.303354529638783]),
        {
            "LC": 0,
            "system:index": "20"
        }),
        ee.Feature(
        ee.Geometry.Point([102.44952924656651, 15.302402473511208]),
        {
            "LC": 0,
            "system:index": "21"
        }),
        ee.Feature(
        ee.Geometry.Point([102.46811159062169, 15.311550312023954]),
        {
            "LC": 0,
            "system:index": "22"
        }),
        ee.Feature(
        ee.Geometry.Point([102.46639497685216, 15.309377219169281]),
        {
            "LC": 0,
            "system:index": "23"
        }),
        ee.Feature(
        ee.Geometry.Point([102.46757514881871, 15.30941861162493]),
        {
            "LC": 0,
            "system:index": "24"
        }),
        ee.Feature(
        ee.Geometry.Point([102.47030027317784, 15.310349939712788]),
        {
            "LC": 0,
            "system:index": "25"
        }),
        ee.Feature(
        ee.Geometry.Point([102.46993549275182, 15.313744078112371]),
        {
            "LC": 0,
            "system:index": "26"
        }),
        ee.Feature(
        ee.Geometry.Point([102.47233875202916, 15.311860752018328]),
        {
            "LC": 0,
            "system:index": "27"
        }),
        ee.Feature(
        ee.Geometry.Point([102.47229583668492, 15.310639685383782]),
        {
            "LC": 0,
            "system:index": "28"
        }),
        ee.Feature(
        ee.Geometry.Point([102.4673391144254, 15.306272761667422]),
        {
            "LC": 0,
            "system:index": "29"
        }),
        ee.Feature(
        ee.Geometry.Point([102.46899135517857, 15.307949174436791]),
        {
            "LC": 0,
            "system:index": "30"
        }),
        ee.Feature(
        ee.Geometry.Point([102.39527199236097, 15.297864275364311]),
        {
            "LC": 0,
            "system:index": "31"
        }),
        ee.Feature(
        ee.Geometry.Point([102.39441368547621, 15.297864275364311]),
        {
            "LC": 0,
            "system:index": "32"
        }),
        ee.Feature(
        ee.Geometry.Point([102.34802219835463, 15.3040733936848]),
        {
            "LC": 0,
            "system:index": "33"
        }),
        ee.Feature(
        ee.Geometry.Point([102.35491960119687, 15.288002814929053]),
        {
            "LC": 0,
            "system:index": "34"
        }),
        ee.Feature(
        ee.Geometry.Point([102.35586373877011, 15.287940719893973]),
        {
            "LC": 0,
            "system:index": "35"
        }),
        ee.Feature(
        ee.Geometry.Point([102.30152821892537, 15.285194072470523]),
        {
            "LC": 0,
            "system:index": "36"
        }),
        ee.Feature(
        ee.Geometry.Point([102.31502509468831, 15.284055645318764]),
        {
            "LC": 0,
            "system:index": "37"
        }),
        ee.Feature(
        ee.Geometry.Point([102.31557730426901, 15.284804223726276]),
        {
            "LC": 0,
            "system:index": "38"
        }),
        ee.Feature(
        ee.Geometry.Point([102.31608155956381, 15.284131516269241]),
        {
            "LC": 0,
            "system:index": "39"
        }),
        ee.Feature(
        ee.Geometry.Point([102.31608692398184, 15.283381186174724]),
        {
            "LC": 0,
            "system:index": "40"
        }),
        ee.Feature(
        ee.Geometry.Point([102.31628004303091, 15.283184547499003]),
        {
            "LC": 0,
            "system:index": "41"
        }),
        ee.Feature(
        ee.Geometry.Point([102.31151655968362, 15.279438982037409]),
        {
            "LC": 0,
            "system:index": "42"
        }),
        ee.Feature(
        ee.Geometry.Point([102.3114951020115, 15.276209883950228]),
        {
            "LC": 0,
            "system:index": "43"
        }),
        ee.Feature(
        ee.Geometry.Point([102.31292203720743, 15.273829427250964]),
        {
            "LC": 0,
            "system:index": "44"
        }),
        ee.Feature(
        ee.Geometry.Point([102.31146291550333, 15.272908286758812]),
        {
            "LC": 0,
            "system:index": "45"
        }),
        ee.Feature(
        ee.Geometry.Point([102.31420949753458, 15.273684529014616]),
        {
            "LC": 0,
            "system:index": "46"
        }),
        ee.Feature(
        ee.Geometry.Point([102.31482104118997, 15.273291233297238]),
        {
            "LC": 0,
            "system:index": "47"
        }),
        ee.Feature(
        ee.Geometry.Point([102.31506780441934, 15.272000831468695]),
        {
            "LC": 0,
            "system:index": "48"
        }),
        ee.Feature(
        ee.Geometry.Point([102.3147888546818, 15.271167742199967]),
        {
            "LC": 0,
            "system:index": "49"
        }),
        ee.Feature(
        ee.Geometry.Point([102.32205042027498, 15.269766073130388]),
        {
            "LC": 0,
            "system:index": "50"
        }),
        ee.Feature(
        ee.Geometry.Point([102.31945975884928, 15.269862318009737]),
        {
            "LC": 0,
            "system:index": "51"
        }),
        ee.Feature(
        ee.Geometry.Point([102.31960996255411, 15.270659271587695]),
        {
            "LC": 0,
            "system:index": "52"
        }),
        ee.Feature(
        ee.Geometry.Point([102.32136426890506, 15.269821116511006]),
        {
            "LC": 0,
            "system:index": "53"
        }),
        ee.Feature(
        ee.Geometry.Point([102.32569458424261, 15.272106431379788]),
        {
            "LC": 0,
            "system:index": "54"
        }),
        ee.Feature(
        ee.Geometry.Point([102.33266487683171, 15.267941220406325]),
        {
            "LC": 0,
            "system:index": "55"
        }),
        ee.Feature(
        ee.Geometry.Point([102.32857638892102, 15.270128051107967]),
        {
            "LC": 0,
            "system:index": "56"
        }),
        ee.Feature(
        ee.Geometry.Point([102.32927912768292, 15.269770973975229]),
        {
            "LC": 0,
            "system:index": "57"
        }),
        ee.Feature(
        ee.Geometry.Point([102.32928985651898, 15.268720439177752]),
        {
            "LC": 0,
            "system:index": "58"
        }),
        ee.Feature(
        ee.Geometry.Point([102.33371013697553, 15.269527747174639]),
        {
            "LC": 0,
            "system:index": "59"
        }),
        ee.Feature(
        ee.Geometry.Point([102.33259433802533, 15.270329877044691]),
        {
            "LC": 0,
            "system:index": "60"
        }),
        ee.Feature(
        ee.Geometry.Point([102.33217054900098, 15.27011252602787]),
        {
            "LC": 0,
            "system:index": "61"
        }),
        ee.Feature(
        ee.Geometry.Point([102.33099574145245, 15.270319527001366]),
        {
            "LC": 0,
            "system:index": "62"
        }),
        ee.Feature(
        ee.Geometry.Point([102.33247082090655, 15.268942348781973]),
        {
            "LC": 0,
            "system:index": "63"
        }),
        ee.Feature(
        ee.Geometry.Point([102.33496442088837, 15.267944754779878]),
        {
            "LC": 0,
            "system:index": "64"
        }),
        ee.Feature(
        ee.Geometry.Point([102.33483031043762, 15.267209892085258]),
        {
            "LC": 0,
            "system:index": "65"
        }),
        ee.Feature(
        ee.Geometry.Point([102.3370243574118, 15.267194366789305]),
        {
            "LC": 0,
            "system:index": "66"
        }),
        ee.Feature(
        ee.Geometry.Point([102.33564570197815, 15.268721015395435]),
        {
            "LC": 0,
            "system:index": "67"
        }),
        ee.Feature(
        ee.Geometry.Point([102.3363752628302, 15.268601988954073]),
        {
            "LC": 0,
            "system:index": "68"
        }),
        ee.Feature(
        ee.Geometry.Point([102.33683123836273, 15.269181595336923]),
        {
            "LC": 0,
            "system:index": "69"
        }),
        ee.Feature(
        ee.Geometry.Point([102.33715846786255, 15.269466222885365]),
        {
            "LC": 0,
            "system:index": "70"
        }),
        ee.Feature(
        ee.Geometry.Point([102.2926389567437, 15.34026763886517]),
        {
            "LC": 0,
            "system:index": "71"
        }),
        ee.Feature(
        ee.Geometry.Point([102.29452723189019, 15.334639020147069]),
        {
            "LC": 0,
            "system:index": "72"
        }),
        ee.Feature(
        ee.Geometry.Point([102.28379839583062, 15.330500233172945]),
        {
            "LC": 0,
            "system:index": "73"
        }),
        ee.Feature(
        ee.Geometry.Point([102.31143587752007, 15.328513586311653]),
        {
            "LC": 0,
            "system:index": "74"
        })])

    image = ee.ImageCollection('COPERNICUS/S1_GRD').filter(ee.Filter.eq('instrumentMode', 'IW')
                                                           ).filterMetadata('resolution_meters', 'equals', 10).filterBounds(big_point).select('VV', 'VH')

    classification_image_date = image.filterDate(
        '2020-9-1', '2020-9-15').mosaic()
    disaster_date = image.filterDate(
        disaster_date[0], disaster_date[1]).mosaic()
    SMOOTHING_RADIUS = 10
    filtered_classification_date = classification_image_date.focal_mean(
        SMOOTHING_RADIUS, 'circle', 'meters')
    filtered_disaster_date = disaster_date.focal_mean(
        SMOOTHING_RADIUS, 'circle', 'meters')
    newfc = water_features.merge(non_flood)
    bands = ['VV', 'VH']
    training = filtered_classification_date.select(bands).sampleRegions(**{
        'collection': newfc,
        'properties': ['LC'],
        'scale': 10
    })
    classifier = ee.Classifier.smileCart().train(**{
        'features': training,
        'classProperty': 'LC',
        'inputProperties': bands
    })
    classified = filtered_disaster_date.select(bands).classify(classifier)
    total_area = ee.Image.pixelArea().addBands(classified.clip(point)).reduceRegion(**{
        'reducer': ee.Reducer.sum().group(**{'groupField': 1}),
        'geometry': classified.clip(point).geometry(),
        'scale': 10,
        'bestEffort': True
    })
    return total_area.getInfo()


def calculate2(point, disaster_date):
    image_point = ee.ImageCollection("COPERNICUS/S1_GRD").filter(ee.Filter.eq("instrumentMode", "IW")
                                                                 ).filterMetadata("resolution_meters", "equals", 10).filterBounds(point).select("VV", "VH")
    try:
        epoch_time = image_point.filterDate(
            disaster_date[0], disaster_date[1]).aggregate_array('system:time_start').getInfo()[-1]
    except:
        epoch_time = 0
    finally:
        return epoch_time


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


def getDateInterval(inputDate):
    inputDateStrp = datetime.datetime.strptime(inputDate, "%Y-%m-%d")
    endDate = inputDateStrp + datetime.timedelta(days=6)
    returnDate = [inputDateStrp.strftime(
        "%Y-%m-%d"), endDate.strftime("%Y-%m-%d")]
    return returnDate


def compute_drought(geom, warrantyDate):
    rawPolygon = ee.Geometry.Polygon(transformPolygon(geom))
    droughtResult = calculate_drought(
        rawPolygon, getDateInterval(warrantyDate))
    epochTime = calculate2(
        rawPolygon, getDateInterval(warrantyDate))
    return droughtResult, epochTime


def compute_flood(geom, warrantyDate):
    rawPolygon = ee.Geometry.Polygon(transformPolygon(geom))
    floodResult = calculate_flood(
        rawPolygon, getDateInterval(warrantyDate))
    epochTime = calculate2(
        rawPolygon, getDateInterval(warrantyDate))
    return floodResult, epochTime


def compute(dataframe):
    for index, row in dataframe.iterrows():
        print("Working On index: ", index)
        if(row['type1'] == 'drought'):
            droughtResult, epochTime = compute_drought(
                row['geom'], row['warranty_dmgdate'])
            if(len(droughtResult['groups']) == 3):
                dryLand = droughtResult['groups'][0]['sum']
                greenLand = droughtResult['groups'][1]['sum']
                notPatty = droughtResult['groups'][2]['sum']
                dataframe.at[index, 'result'] = str(
                    dryLand*100/(dryLand+greenLand))
            elif(len(droughtResult['groups']) == 0):
                dataframe.at[index, 'result'] = "No Sat image"
            elif(len(droughtResult['groups']) == 1):
                if(droughtResult['groups'][0]['group'] == 0):
                    dataframe.at[index, 'result'] = "100"
                else:
                    dataframe.at[index, 'result'] = "0"
            else:  # group count == 2
                if(droughtResult['groups'][0]['group'] == 0 and droughtResult['groups'][1]['group'] == 1):
                    dryLand = droughtResult['groups'][0]['sum']
                    greenLand = droughtResult['groups'][1]['sum']
                    dataframe.at[index, 'result'] = str(
                        dryLand*100/(dryLand+greenLand))
                elif((droughtResult['groups'][0]['group'] == 0 and droughtResult['groups'][1]['group'] == 3)):
                    dataframe.at[index, 'result'] = "100"
                else:
                    dataframe.at[index, 'result'] = "0"
            if(epochTime != 0):
                normalTime = datetime.datetime.fromtimestamp(
                    epochTime/1000).strftime("%Y-%m-%d")
                dataframe.at[index, 'sat_time'] = normalTime
            else:
                dataframe.at[index, 'sat_time'] = "No Time"
        else:
            floodResult, epochTime = compute_flood(
                row['geom'], row['warranty_dmgdate'])
            if(len(floodResult['groups']) == 2):
                flooded = floodResult['groups'][0]['sum']
                notFlooded = floodResult['groups'][1]['sum']
                dataframe.at[index, 'result'] = str(
                    flooded*100/(flooded+notFlooded))
            elif(len(floodResult['groups']) == 0):
                dataframe.at[index, 'result'] = "No Sat image"
            else:
                if(floodResult['groups'][0]['group'] == 0):
                    dataframe.at[index, 'result'] = "100"
                else:
                    dataframe.at[index, 'result'] = "0"
            if(epochTime != 0):
                normalTime = datetime.datetime.fromtimestamp(
                    epochTime/1000).strftime("%Y-%m-%d")
                dataframe.at[index, 'sat_time'] = normalTime
            else:
                dataframe.at[index, 'sat_time'] = "No Time"
    return dataframe


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
    df2.to_csv(r'./validationcartdrought1.csv',
               index=False, header=True)
