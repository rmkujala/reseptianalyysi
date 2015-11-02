import pandas as pd
from collections import defaultdict

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

    def __init__(self, ref_year=1995):
        self.ref_year = ref_year
        data = pd.read_csv("data/consumer_price_index.csv", sep=",",
                           comment="#", header=0,
                           names=["year", "desc", "value"],
                           dtype={'year': int, "desc": str, "value":float}
                           )
        convert_dict = {}

        for row_index, row in data.iterrows():
            year = row['year']
            value = row['value']
            # take care of euro
            eur_to_fim = 5.94573
            if year < 2002:
                value = value  * eur_to_fim
            convert_dict[year] = 1./value

        # normalize
        norm = convert_dict[ref_year]
        for key, value in convert_dict.items():
            convert_dict[key] = value/norm
        self.convert_dict = convert_dict

    def money_to_value(self, value, year):
        assert year in self.convert_dict
        return value*self.convert_dict[year]


def get_atc_label(atc_code, atc_classes, atc_class_labels):
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
    ref_year = 2010
    c = Converter(ref_year)
    years = range(1995, 2013)
    for year in years:
        print year, 1, "->", c.money_to_value(1, year)


    # print c.money_to_value(100, 1998)
    # print c.money_to_value(100, 2010)

    dp = "data/"
    orig_data_files = [
        dp+"kela_reseptilaakkeet_1995-1999_15.11.2010.csv",
        dp+"kela_reseptilaakkeet_2000-2004_15.11.2010.csv",
        dp+"kela_reseptilaakkeet_2005-2009_15.11.2010.csv",
    ]

    # the original atc_classes
    # the corresponding groups we assigne them to
    atc_classes = [["A10A"], ["A10B", "A10X"]]
    atc_class_labels = ["A10A", "A10_others"]

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
        dtypes = {
            "Jnro": int,
            "Multiple": str,
            "SAIR": str,
            "LAJI": str,
            "VNRO": str,
            "KUST": float,
            "KORV": float,
            "ATC": str,
            "KAKORV": str,
            "ostopv": str,
            "plkm": int,
            "OstoDate": str,
            "Year": int,
            "filter_": str
            }

        usecols = ["Jnro", "KUST", "KORV", "plkm", "Year"]
        # filter_ is only in 1995
        if not "1995" in fname:
            names = names[:-1]
            dtypes.pop("filter_")
        # skiprows = 1 -> skip header row (names are specified)
        df = pd.read_csv(fname, sep=";", names=names, skiprows=1)#, nrows=1000)#, usecols=usecols)
        datasets.append(df)

    dataset = pd.concat(datasets, ignore_index=True)


    # dictionary from labels to classes
    atc_class_label_to_classes = \
        dict(keys=atc_class_labels, values=atc_classes)

    # variables we are interested in each transaction
    # (these are summed to) yearly values
    variables = ["plkm", "KUST", "KORV"]

    year_acl_var_to_column_name = dict()

    # get column headers
    columns = ["Jnro"] # id label
    years = range(1995,2009+1) # +1 to get also year 2009

    for year in years:
        for acl in atc_class_labels:
            for v in variables:
                col = str(year)+"_"+acl+"_"+v
                columns.append(col)
                year_acl_var_to_column_name[(year,acl, v)] = col

    res_dict_dict = defaultdict(lambda: defaultdict(lambda:0))

    # keys should be: [jnro][(year, acl, v)]
    gb = dataset.groupby(['Jnro', 'Year'])

    # convert money to values
    c = Converter(ref_year)

    for (jnro, year), rows in gb:
        year = int(year)
        # print jnro, year
        for row_i, row in rows.iterrows():
            acl = get_atc_label(row.ATC, atc_classes, atc_class_labels)
            if acl:
                kust_value = c.money_to_value(float(row.KUST), year)
                korv_value = c.money_to_value(float(row.KORV), year)
                plkm_value = int(row.plkm)
                if (int(jnro) == 70680) and year == 1995:
                    print plkm_value, row.ATC
                vals = [kust_value, korv_value, plkm_value]
                kust_key = (year, acl, "KUST")
                korv_key = (year, acl, "KORV")
                plkm_key = (year, acl, "plkm")
                keys = [kust_key, korv_key, plkm_key]
                for key, val in zip(keys, vals):
                    res_dict_dict[jnro][key] += val


    res_df = pd.DataFrame(columns=columns)
    # index=res_dict_dict.keys(), dtype=[str]+[float]*(len(columns)-1),

    # res_dict is ready, need to transform it into a dataframe still
    for jnro, jnro_dict in res_dict_dict.items():
        row_dict = {'Jnro': int(jnro)}
        for key, col_name in year_acl_var_to_column_name.items():
            row_dict[col_name] = jnro_dict[key]
        res_df = res_df.append(row_dict, ignore_index=True)

    res_df.to_csv("data/yearly_sums_A10_index_and_euro_corrected.csv")
