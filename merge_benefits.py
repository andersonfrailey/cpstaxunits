"""
Merge imputed benefits from the open source C-TAM model to the three CPS files
"""
import pandas as pd
from dask import compute, delayed


def merge(year, cps):
    """
    Merge the benefit variables onto the CPS files.
    """

    # read in benefit imputations
    mcaid_prob_str = f'data/medicaid_prob{year}.csv'
    mcaid_prob = pd.read_csv(mcaid_prob_str)
    mcaid_str = f'data/medicaid{year}.csv'
    mcaid = pd.read_csv(mcaid_str)

    mcare_prob_str = f'data/medicare_prob{year}.csv'
    mcare_prob = pd.read_csv(mcare_prob_str)
    mcare_str = f'data/medicare{year}.csv'
    mcare = pd.read_csv(mcare_str)

    vb_str = f'data/VB_Imputation{year}.csv'
    vb = pd.read_csv(vb_str)

    snap_str = f'data/SNAP_Imputation_{year}.csv'
    snap = pd.read_csv(snap_str, usecols=['h_seq', 'probs', 'snap_impute'])
    snap = snap.rename(columns={'probs': 'snap_probs'})

    ssi_str = f'data/SSI_Imputation{year}.csv'
    ssi = pd.read_csv(ssi_str)

    ss_str = f'data/SS_augmentation_{year}.csv'
    ss = pd.read_csv(ss_str)

    h_str = 'data/Housing_Imputation_logreg_{}.csv'.format(year)
    housing = pd.read_csv(h_str,
                          usecols=['fh_seq', 'ffpos', 'housing_impute',
                                   'probs'])
    housing = housing.rename(columns={'probs': 'housing_probs'})
    tanf_str = 'data/TANF_Imputation_{}.csv'.format(year)
    tanf = pd.read_csv(tanf_str, usecols=['peridnum', 'tanf_impute', 'probs'])
    ui_str = 'data/UI_imputation_logreg_{}.csv'.format(year)
    ui = pd.read_csv(ui_str, usecols=['peridnum', 'UI_impute', 'probs'])
    w_str = 'data/WIC_imputation_{}_logreg_{}.csv'
    wic_children = pd.read_csv(w_str.format('children', year),
                               usecols=['peridnum', 'WIC_impute', 'probs'])
    wic_infants = pd.read_csv(w_str.format('infants', year),
                              usecols=['peridnum', 'WIC_impute', 'probs'])
    wic_women = pd.read_csv(w_str.format('women', year),
                            usecols=['peridnum', 'WIC_impute', 'probs'])

    # merge housing and snap
    cps_merged = cps.merge(housing, on=['fh_seq', 'ffpos'], how='left')
    cps_merged = cps_merged.merge(snap, on='h_seq', how='left')
    cps_merged = cps_merged.fillna(0.)
    # add other variables
    cps_merged['mcaid_probs'] = mcaid_prob['prob']
    cps_merged['MedicaidX'] = mcaid['MedicaidX']
    cps_merged['mcare_probs'] = mcare_prob['prob']
    cps_merged['MedicareX'] = mcare['MedicareX']
    cps_merged['vb_probs'] = vb['prob']
    cps_merged['vb_impute'] = vb['vb_impute']
    cps_merged['ssi_probs'] = ssi['probs']
    cps_merged['ssi_impute'] = ssi['ssi_impute']
    cps_merged['ss_probs'] = ss['Prob_Received']
    cps_merged['ss_val_y'] = ss['ss_val']
    cps_merged['tanf_impute'] = tanf['tanf_impute']
    cps_merged['tanf_probs'] = tanf['probs']
    cps_merged['ui_impute'] = ui['UI_impute']
    cps_merged['ui_probs'] = ui['probs']
    cps_merged['wic_impute'] = (wic_children['WIC_impute'] +
                                wic_infants['WIC_impute'] +
                                wic_women['WIC_impute'])
    cps_merged['wic_probs'] = wic_women['probs']
    print('Saving {} Data'.format(year))
    cps_merged.to_csv(f'data/cps_{year}_aug.csv', index=False)
    return cps_merged


def BenefitMerge(cps_2013, cps_2014, cps_2015):
    """
    Main logic for merging imputed benefits onto the CPS files
    """

    # pass files to merge function
    cps2013merge = delayed(merge)(2013, cps_2013)
    cps2014merge = delayed(merge)(2014, cps_2014)
    cps2015merge = delayed(merge)(2015, cps_2015)
    lazy_merge = [cps2013merge, cps2014merge, cps2015merge]

    results = compute(*lazy_merge)

    return results
