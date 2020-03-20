# -*- coding: utf-8 -*-
import re

CN_NUM = {
    u'〇': 0,
    u'一': 1,
    u'二': 2,
    u'三': 3,
    u'四': 4,
    u'五': 5,
    u'六': 6,
    u'七': 7,
    u'八': 8,
    u'九': 9,
    u'零': 0,
    u'壹': 1,
    u'贰': 2,
    u'叁': 3,
    u'肆': 4,
    u'伍': 5,
    u'陆': 6,
    u'柒': 7,
    u'捌': 8,
    u'玖': 9,
    u'貮': 2,
    u'两': 2,
}

CN_UNIT = {
    u'十': 10,
    u'拾': 10,
    u'百': 100,
    u'佰': 100,
    u'千': 1000,
    u'仟': 1000,
    u'万': 10000,
    u'萬': 10000,
    u'亿': 100000000,
    u'億': 100000000,
    u'兆': 1000000000000,
}


# 将地址字符串里面的汉字数字全都换成阿拉伯数字
def cn2dig(matched):
    cn = matched.group(0)
    # print type(matched.group(0)), matched.group(0)
    lcn = list(cn)
    unit = 0  # 当前的单位
    ldig = []  # 临时数组
    while lcn:
        cndig = lcn.pop()

        if CN_UNIT.has_key(cndig):
            unit = CN_UNIT.get(cndig)
            if unit == 10000:
                ldig.append('w')  # 标示万位
                unit = 1
            elif unit == 100000000:
                ldig.append('y')  # 标示亿位
                unit = 1
            elif unit == 1000000000000:  # 标示兆位
                ldig.append('z')
                unit = 1

            continue

        else:
            dig = CN_NUM.get(cndig)

            if unit:
                dig = dig * unit
                unit = 0

            ldig.append(dig)

    if unit == 10:  # 处理10-19的数字
        ldig.append(10)

    ret = 0
    tmp = 0

    while ldig:
        x = ldig.pop()

        if x == 'w':
            tmp *= 10000
            ret += tmp
            tmp = 0

        elif x == 'y':
            tmp *= 100000000
            ret += tmp
            tmp = 0

        elif x == 'z':
            tmp *= 1000000000000
            ret += tmp
            tmp = 0

        else:
            tmp += x

    ret += tmp
    return str(ret)


class BuildingRecognizer(object):

    PATTERN_RECOGNIZE_BASEMENT = re.compile(ur'-\d楼|-\d层|-\d[fF]')
    PATTERN_RECOGNIZE = re.compile(ur'\d+楼|\d{3,4}$|\d{3,4} |\d{3,4}室|\d{3,4}房|\d+层|\d+[fF]')
    REPLACE_RECOGNIZE = re.compile(ur'楼|室|层|房|[A-Za-z]| ')
    CHINESE_NUM_RECOGNIZE = re.compile(ur'[一二三四五六七八九十]+')
    MINUS_NUM_RECOGNIZE = re.compile(ur'[Bb负]')

    PATTERN_RECOGNIZE_BUILDING = re.compile(ur'\d+号楼|\d+栋|\d+幢')
    REPLACE_RECOGNIZE_BUILDING = re.compile(ur'号楼|栋|幢')

    def get_building_floor(self, address):
        # 将汉语的数字转换成阿拉伯数字
        address = self.CHINESE_NUM_RECOGNIZE.sub(cn2dig, address)
        address = self.MINUS_NUM_RECOGNIZE.sub('-', address)
        res_basement = self.PATTERN_RECOGNIZE_BASEMENT.findall(address)
        res = self.PATTERN_RECOGNIZE.findall(address)
        res = res_basement + res
        if res:
            floor = self.REPLACE_RECOGNIZE.sub("", res[0])
            if len(floor) > 2:
                return int(floor[0:-2])
            else:
                return int(floor)
        else:
            return 0

    def get_building_num(self, address):
        """获取楼号"""
        address = self.CHINESE_NUM_RECOGNIZE.sub(cn2dig, address)
        address = self.MINUS_NUM_RECOGNIZE.sub('-', address)
        building = self.PATTERN_RECOGNIZE_BUILDING.findall(address)
        result_building = -999
        if building:
            result_building = self.REPLACE_RECOGNIZE_BUILDING.sub("", building[0])
            result_building = int(result_building)
        return result_building



class ETABuildingRecognizer(BuildingRecognizer):
    """
    加了几号楼提取
    """
    PATTERN_RECOGNIZE = re.compile(ur'\d+楼|\d{3,4}$|\d{3,4} |\d{3,4}室|\d{3,4}房|\d+层|\d+[A-Za-z]')




if __name__ == '__main__':
    # address = u'淮海西路241路近番禺路胸科医院3号楼八楼护士站十五楼'
    # address = u'淮海西路241路近番禺路胸科医院3号楼护士站'
    # address = u'会展时代丽影广场新港中路352号1803'
    address = u'前进路与颖河路交叉口东南角负一楼'
    # address = u'四川省成都市郫都区郫筒镇望丛中路1092号-1F'
    address = u'深圳市龙岗区深圳市龙岗中心医院内儿科住院楼13楼19床'

    build = ETABuildingRecognizer()
    floor = build.get_building_floor(address)
    print '[*] address: ', address
    print '[*] floor: ', floor
    pass


"""

if __name__ == '__main__':
    address_list = [u'淮海西路241路近番禺路胸科医院3号楼八楼护士站十五楼',
                    u'会展时代丽影广场新港中路352号1803',
                    u'淮海西路241路近番禺路胸科医院3号楼护士站',
                    u'前进路与颖河路交叉口东南角负一楼',
                    u'四川省成都市郫都区郫筒镇望丛中路1092号-1F',
                    u'四川省成都市郫都区郫筒镇望丛中路13号楼43D',
                    u'四川省成都市郫都区郫筒镇望丛中路十八号楼43D',
                    u'万源路2289弄2号18039',
                    u'洛川东路352弄小区 (6号302室（门铃按804）)',
                    u'松江大学城四期学生公寓文汇路1000弄 10号楼4023',
                    u'网鱼网咖(文汇店)73号机',
                    u'城市15号楼'
                    ]


    build = BuildingRecognizer2()
    for i in tqdm(range(1)):
        for address in address_list:
            which_building, which_floor = build.get_building_floor(address)
            print '[*] address: ', address,
            print '[*] floor: ', which_building, which_floor

"""