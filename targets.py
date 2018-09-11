"""
Script to handle state level targeting of certain variables
"""
import pandas as pd


def targets(cps, state_targets):
    """
    Uses state aggregates to adjust totals for certain variables

    Parameters
    ----------
    cps: CPS file
    state_targets: Pandas DataFrame consisting of individual income stats from
                   the IRS. The raw data used to collect this information can
                   be found here:
                   https://www.irs.gov/pub/irs-soi/14in54cmcsv.csv
    """
    # create single dividends variable
    state_targets['divs'] = state_targets['A00600'] + state_targets['A00650']

    # create lists with each state and their corresponding fips code
    # states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL',
    #           'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME',
    #           'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH',
    #           'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI',
    #           'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI',
    #           'WY']
    fips = [1, 2, 4, 5, 6, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 21,
            22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37,
            38, 39, 40, 41, 42, 44, 45, 46, 47, 48, 49, 50, 51, 53, 54, 55, 56]
    fips_dict = {'AK': 2, 'AL': 1, 'AR': 5, 'AZ': 4, 'CA': 6, 'CO': 8, 'CT': 9,
                 'DC': 11, 'DE': 10, 'FL': 12, 'GA': 13, 'HI': 15, 'IA': 19,
                 'ID': 16, 'IL': 17, 'IN': 18, 'KS': 20, 'KY': 21, 'LA': 22,
                 'MA': 25, 'MD': 24, 'ME': 23, 'MI': 26, 'MN': 27, 'MO': 29,
                 'MS': 28, 'MT': 30, 'NC': 37, 'ND': 38, 'NE': 31, 'NH': 33,
                 'NJ': 34, 'NM': 35, 'NV': 32, 'NY': 36, 'OH': 39, 'OK': 40,
                 'OR': 41, 'PA': 42, 'RI': 44, 'SC': 45, 'SD': 46, 'TN': 47,
                 'TX': 48, 'UT': 49, 'VA': 51, 'VT': 50, 'WA': 53, 'WI': 55,
                 'WV': 54, 'WY': 56}
    var_dict = {'A00200': ('wass', 'wasp'), 'A00300': ('intstp', 'intsts'),
                'A00600': ('dbep', 'dbes'), 'A00900': ('bilp', 'bils'),
                'A01000': 'CGAGIX', 'A01400': 'TIRAD',
                'A01700': ('pensionsp', 'pensionss'), 'A02300': 'ucomp',
                'A03300': 'KEOGH', 'A03270': 'SEHEALTH',
                'A03150': 'ADJIRA', 'A03210': 'SLINT', 'A03240': 'DPAD'}
    # irs_vars = ('A00200', 'A00300', 'divs', 'A00900', 'A01000', 'A01400',
    #             'A01700', 'A02300', 'A03300', 'A03270', 'A03150', 'A03210',
    #             'A03240')
    cps_vars = [('wasp', 'wass'), ('intstp', 'intsts'), ('dbep', 'dbes'),
                'CGAGIX', 'TIRAD', ('pensionsp', 'pensionss'), 'ucomp',
                'KEOGH', 'SEHEALTH', 'ADJIRA', 'SLINT', 'DPAD']

    # # dictionary to hold factors
    factor_dict = {}
    # # loop through each targeted variable
    # for irs_var, cps_var in zip(irs_vars, cps_vars):
    #     factor_dict[irs_var] = []
    #     # loop through each state
    #     for state, fip in zip(states, fips):
    #         target = state_targets[irs_var][state] * 1000
    #         try:
    #             cps_sum = ((cps[cps['xstate'] == fip][cps_var[0]] +
    #                         cps[cps['xstate'] == fip][cps_var[1]]) *
    #                        cps[cps['xstate'] == fip]['wt']).sum()
    #         except KeyError:
    #             cps_sum = (cps[cps['xstate'] == fip][cps_var] *
    #                        cps[cps['xstate'] == fip]['wt']).sum()
    #         factor = target / cps_sum
    #         factor_dict[irs_var].append(factor)

    # new iteration
    for var in var_dict:
        factor_dict[var] = []
        cps_vars = var_dict[var]
        # loop through each state
        for state in fips_dict:
            fips_code = fips_dict[state]
            target = state_targets[var][state]
            # for variables where income is split, use the sum of both for
            # factor calculations
            try:
                cps_sum = ((cps[cps_vars[0]][cps['xstate'] == fips_code] +
                            cps[cps_vars[1]][cps['xstate'] == fips_code]) *
                           cps['wt'][cps['xstate'] == fips_code]).sum()
            except KeyError:
                cps_sum = (cps[cps_vars][cps['xstate'] == fips_code] *
                           cps['wt'][cps['xstate'] == fips_code]).sum()
            cps_sum /= 1000
            factor = target / cps_sum
            factor_dict[var].append(factor)

    # create a dataframe from the factor dictionary
    factor_df = pd.DataFrame(factor_dict)
    factor_df.index = fips

    # apply factors to specified variables
    # for irs_var, cps_var in zip(irs_vars, cps_vars):
    #     factor_array = factor_df[irs_var][cps['xstate']].values
    #     try:
    #         cps[cps_var[0]] *= factor_array
    #         cps[cps_var[1]] *= factor_array
    #     except KeyError:
    #         cps[cps_var] *= factor_array

    # new iteration
    for var in var_dict:
        factor_array = factor_df[var][cps['xstate']].values
        cps_vars = var_dict[var]
        try:
            cps[cps_vars[0]] *= factor_array
            cps[cps_vars[0]] *= factor_array
        except KeyError:
            cps[cps_vars] *= factor_array

    # recalculate certain variables
    cps['was'] = cps['wasp'] + cps['wass']
    cps['intst'] = cps['intstp'] + cps['intsts']
    cps['dbe'] = cps['dbep'] + cps['dbes']
    cps['bil'] = cps['bilp'] + cps['bils']
    cps['pensions'] = cps['pensionsp'] + cps['pensionss']

    return cps
