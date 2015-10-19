import pandas as pd


# start data 1995, ....,  2009

# data_fields_to_use = [
#     "JNro", # same across the three files
#     "KUST", #
#     "OstoDate",
#     "YEAR",
#     "ATC"
# ]

# ATC-luokat:
#      1. A10A
#      2. A10B, A10X


# Variaabelit: (naista summat)
# PKLM, KUST, KORV

# Vain ne, joilla jotain dataa.

# Vuosittainen summa (mista luvuista) / henkilo

# laakeaineet (ATC-koodit):

# JNRO,
# KUST_A10A_1995,
# KUST_A10_muut_1995,
# pklm_A10A_1995,
# pklm_A10_muut_1995,
# KORV_A10A_1995,
# KORV_A10_muut_1995,
# KUST_A10A_1996,
# KUST_A10_muut_1996,
# pklm_A10A_1996,
# pklm_A10_muut_1996,
# KORV_A10A_1996,
# KORV_A10_muut_1996,



class Converter():

    def __init__(self, ref_year=2008):
        data = pd.read_csv("data/consumer_price_index.csv", sep=",", comment="#", header=0, names=["year", "desc", "value"])
        convert_dict = {}

        self.ref_year = ref_year
        norm = float(data[data['year']==ref_year]['value'])


        for row_index, row in data.iterrows():
            # print row[0], row[1]
            # for i in row:
            #     print i
            year = row['year']
            value = row['value']
            # take care of euro
            eur_to_fim = 5.94573
            if year < 2002:
                value = value / eur_to_fim
            convert_dict[year] = value/float(norm)
            self.convert_dict = convert_dict

    def money_to_value(self, value, year):
        assert year in self.convert_dict
        return value/self.convert_dict[year]


def get_atc_label(atc_code):
    atc_classes = [["A10A"], ["A10B", "A10X"]]
    atc_class_labels = ["A10A", "A10_others"]
    res = set()
    for atc_subc, acl in zip(atc_classes, atc_class_labels):
        for atc_start in atc_subc:
            if atc_start == atc_code[:len(atc_start)]:
                res.add(acl)
    assert len(res) <= 1
    if len(res) == 0:
        return None
    else:
        return list(res)[0]




if __name__ == "__main__":
    c = Converter(2008)


    # print c.money_to_value(100, 1998)
    # print c.money_to_value(100, 2010)

    dp = "data/"
    orig_data_files = [
        dp+"kela_reseptilaakkeet_1995-1999_15.11.2010.csv",
        dp+"kela_reseptilaakkeet_2000-2004_15.11.2010.csv",
        dp+"kela_reseptilaakkeet_2005-2009_15.11.2010.csv",
    ]


    # combine original datasets
    datasets = []
    for fname in orig_data_files:
        names = [
            "Jnro",
            "Multiple",
            "SAIR",
            "LAJI",
            "VNRO",
            "KUST",
            "KORV",
            "ATC",
            "KAKORV",
            "ostopv",
            "plkm",
            "OstoDate",
            "Year",
            "filter_"
        ]
        if not "1995"in fname:
            names = names[:-1]
        datasets.append(pd.read_csv(fname, sep=";", names=names))

    dataset = pd.concat(datasets, ignore_index=True)

    atc_classes = [["A10A"], ["A10B", "A10X"]]
    atc_class_labels = ["A10A", "A10_others"]

    atc_class_label_to_classes = \
        dict(keys=atc_class_labels, values=atc_classes)

    variables = ["plkm", "KUST", "KORV"]

    # get column headers
    columns = ["Jnro"] # id label
    years = range(1995,2009+1) # +1 to get also year 2009
    for year in years:
        for acl in atc_class_labels:
            for v in variables:
                col = str(year)+"_"+acl+"_"+v
                columns.append(col)
    # print columns

    # 1. get data for jnr
    jnro_label = dataset.columns[-1]

    res_dict = []

    # jnro->year->atc->
    print dataset.columns

    years = dataset[jnro_label]

    gb = dataset.groupby(['Jnro', 'Year'])

    result_ds = \
        pd.DataFrame(data=None, index=None, columns=columns, dtype=int)


    for (jnro, year), rows in gb:
        for row_i, row in rows.iterrows():
            if get_atc_label:
                print gb

        # for row_i, row in rows.iterrows():
        #     break
        # break


    # 2. loop over jnro data
    # 3. store data to dataframe
    # (4. sort by jnro)

