"""
Create tax units using the 2013, 2014, and 2015 CPS
"""
import pandas as pd
import numpy as np
from tqdm import tqdm


class Returns(object):
    """
    Class used to create tax units
    """
    def __init__(self, cps, year):
        """
        Parameters
        ----------
        cps: CPS file used
        year: year the CPS file is from
        """
        # Set CPS and household numbers in the file
        self.cps = cps
        self.year = year
        self.h_nums = np.unique(self.cps['h_seq'].values)
        self.nunits = 0

        # Set filing income thresholds
        self.single = 10150.
        self.single65 = 11700.
        self.hoh = 13050.
        self.hoh65 = 14600.
        self.joint = 20300.
        self.joint65one = 21500.
        self.joint65both = 22700.
        self.widow = 16350.
        self.widow65 = 17550.
        self.depwages = 0.
        self.depTotal = 1000.
        # Wage thresholds for non-dependent filers
        self.wage1 = 1000.
        self.wage2 = 250.
        self.wage2nk = 10000.
        self.wage3 = 1.
        # Dependent exemption
        self.depExempt = 3950

        # Lists to hold tax units in each household
        self.house_units = list()
        self.tax_units = list()

        # Set flags in CPS file
        self.cps['h_flag'] = False  # Tax unit head flag
        self.cps['s_flag'] = False  # Tax unit spouse flag
        self.cps['d_flag'] = False  # Tax Unit Dependent flag
        self.cps['t_flag'] = False  # Tax unit flag
        self.cps['flag'] = False  # General flag

        # benefit variables
        self.ben_vars = ['SSI_PROB', 'SSI_VAL', 'SS_PROB', 'SS_VAL',
                         'SNAP_PROB', 'SNAP_VAL', 'MCARE_PROB', 'MCARE_VAL',
                         'MCAID_PROB', 'MCAID_VAL', 'VB_PROB', 'VB_VAL',
                         'TANF_PROB', 'TANF_VAL', 'HOUSING_PROB',
                         'HOUSING_VAL', 'UI_PROB', 'UI_VAL', 'WIC_PROB',
                         'WIC_VAL']

    def computation(self):
        """
        Construct CPS tax units based on type of household
        Parameters
        ----------
        Returns
        -------
        None
        """
        # In 2015 alimony was moved to other income
        if self.year == 2015:
            self.cps['alm_val'] = np.where(self.cps['oi_off'] == 20,
                                           self.cps['oi_val'], 0.)

        # start by looping through each household
        for num in tqdm(self.h_nums):
            self.nunits = 0
            # clear house_units list to avoid double counting tax units
            del self.house_units[:]
            # self.house_units.clear() for when we move to 3.6 officially
            # only use the households with that h_seq
            household = self.cps[self.cps['h_seq'] == num]
            household = household.sort_values('a_lineno', kind='mergesort')
            house_dicts = household.to_dict('records')
            head = house_dicts[0]  # head record for the household

            # determine household type to determine how unit will be created
            # TODO: why not also include h_type = 3 and 4 in single?
            single = ((head['h_type'] == 6 or
                       head['h_type'] == 7) and
                      head['h_numper'] == 1)
            group = head['h_type'] == 9
            # TODO: why not include h_type == 8 in group? define nonfamily
            # single persons living alone
            if single:
                self.house_units.append(self.create(head, house_dicts))
            # create a unit for each person in a group household
            elif group:
                for person in house_dicts:
                    self.house_units.append(self.create(person, house_dicts))
            else:  # all other household types
                for person in house_dicts:
                    # only create a new unit if that person is not flagged
                    not_flagged = (not person['h_flag'] and
                                   not person['s_flag'] and
                                   not person['d_flag'])
                    if not_flagged:
                        self.house_units.append(self.create(person,
                                                            house_dicts))

                    # check if the person is a dependent and must file
                    if not person['s_flag'] and person['d_flag']:
                        if self.must_file(person):
                            self.house_units.append(self.create(person,
                                                                house_dicts))

                    # check for dependents in the household
                    if self.nunits > 1:
                        self.search()

            # check head of household status
            map(self.hhstatus, self.house_units)

            # add units to full tax unit list
            for unit in self.house_units:
                if unit['t_flag']:
                    self.tax_units.append(self.output(unit, house_dicts))

        final_output = pd.DataFrame(self.tax_units)
        num_units = len(final_output)
        print('There are {:,} tax units in the {} file'.format(num_units,
                                                               self.year))
        return(final_output)

    def create(self, record, house):
        """
        Create an actual tax unit
        Parameters
        ----------
        record: dictionary record for the head of the unit
        house: list of dictionaries, each containing a member of the household

        Returns
        -------
        Completed tax unit
        """
        unit = {}
        unit['year'] = self.year - 1
        self.nunits += 1
        unit['flag'] = True

        # income items
        unit['agi_head'] = record['agi']
        unit['agi'] = unit['agi_head']
        unit['was'] = record['wsal_val']
        unit['wasp'] = unit['was']
        unit['intst'] = record['int_val']
        unit['intstp'] = unit['intst']
        unit['dbe'] = record['div_val']
        unit['dbep'] = unit['dbe']
        unit['alimony'] = record['alm_val']
        unit['alimonyp'] = unit['alimony']
        unit['bil'] = record['semp_val']
        unit['bilp'] = unit['bil']
        unit['pensions'] = record['rtm_val']
        unit['pensionsp'] = unit['pensions']
        unit['rents'] = record['rnt_val']
        unit['rentsp'] = unit['rents']
        unit['fil'] = record['frse_val']
        unit['filp'] = unit['fil']
        unit['ucomp'] = record['uc_val']
        unit['socsec'] = record['ss_val']

        # weights and flags
        unit['wt'] = record['fsup_wgt']
        unit['ifdept'] = record['d_flag']  # dependent flag
        unit['h_flag'] = True  # tax unit head flag

        # CPS identifiers
        unit['xhid'] = record['h_seq']
        unit['xfid'] = record['ffpos']
        unit['xpid'] = record['ph_seq']
        unit['xstate'] = record['gestfips']
        unit['xregion'] = record['gereg']
        unit['a_lineno'] = record['a_lineno']

        # CPS evaluation crieria (head)
        # CPS identifiers
        unit['xhid'] = record['h_seq']
        unit['xfid'] = record['ffpos']
        unit['xpid'] = record['ph_seq']
        unit['xstate'] = record['gestfips']
        unit['xregion'] = record['gereg']
        # CPS evaluation criteria (head)
        unit['zifdep'] = record['d_flag']   # Tax unit dependent flag
        unit['zntdep'] = 0
        unit['zhhinc'] = record['hhinc']
        unit['zagept'] = record['a_age']
        unit['zagesp'] = 0
        unit['zoldes'] = 0
        unit['zyoung'] = 0
        unit['zworkc'] = record['wc_val']
        unit['zsocse'] = record['ss_val']
        unit['zssinc'] = record['ssi_val']
        unit['zpubas'] = record['paw_val']
        unit['zvetbe'] = record['vet_val']
        unit['zchsup'] = 0
        unit['zfinas'] = 0
        unit['zdepin'] = 0
        unit['zowner'] = 0
        unit['zwaspt'] = record['wsal_val']
        unit['zwassp'] = 0
        # blindness indicators
        unit['blind_head'] = 0
        unit['blind_spouse'] = 0
        if record['pediseye'] == 1:
            unit['blind_head'] = 1
        # homeownership flag
        if self.nunits == 1 and record['h_tenure'] == 1:
            unit['zownder'] = 1

        # marital status
        ms = record['a_maritl']
        unit['ms_type'] = 1
        if ms == 1 or ms == 2 or ms == 3:
            unit['ms_type'] = 2
        unit['sp_ptr'] = int(record['a_spouse'])  # pointer to spouse record
        unit['relcode'] = record['a_exprrp']
        unit['ftype'] = record['ftype']
        unit['ageh'] = record['a_age']
        unit['agede'] = 0
        if unit['ageh'] >= 65:
            unit['agede'] = 1
        unit['ages'] = 0
        # Other age related variables
        unit['nu05'] = 0  # only checked for dependents
        unit['nu13'] = 0  # only checked for dependents
        unit['nu18_dep'] = 0
        unit['nu18'] = 0
        unit['n1820'] = 0
        unit['n21'] = 0
        unit['elderly_dependent'] = 0
        unit['f2441'] = 0
        unit['EIC'] = 0
        unit['n24'] = 0
        self.check_age(unit, unit['ageh'])
        unit['depne'] = 0
        # define all spouse income variables
        unit['ages'] = np.nan  # age of spouse
        unit['agi_spouse'] = 0.  # spouse's agi
        unit['wass'] = 0.  # spouse's wage
        unit['intsts'] = 0.  # spouse's interest income
        unit['dbes'] = 0.  # spouse's dividend income
        unit['alimonys'] = 0.  # spouse's alimony
        unit['bils'] = 0.  # spouse's business income
        unit['pensionss'] = 0.  # spouse's pension
        unit['rentss'] = 0.  # spouse's rental income
        unit['fils'] = 0.  # spouse's farm income
        unit['ucomps'] = 0.  # spouse's unemployment income

        # single and separated individuals
        if unit['ms_type'] == 1:
            unit['js'] = 1
            # certain single individuals can file as head of household
            # TODO: check and see if actually needed
            if ((record['h_type'] == 6 or
                 record['h_type'] == 7) and record['h_numper'] == 1):
                if ms == 6:
                    unit['js'] = 1
        elif unit['ms_type'] == 2:
            unit['js'] = 2
            if unit['sp_ptr'] != 0:
                # locate spouse's record
                for person in house:
                    sp = (person['a_lineno'] == record['a_spouse'] and
                          person['a_spouse'] == record['a_lineno'])
                    if sp:
                        spouse = person
                        break
                unit['ages'] = spouse['a_age']
                if unit['ages'] >= 65:
                    unit['agede'] += 1
                self.check_age(unit, unit['ages'])
                unit['agi_spouse'] = spouse['agi']
                unit['agi'] += unit['agi_spouse']
                unit['wass'] = spouse['wsal_val']
                unit['was'] += unit['wass']
                unit['intsts'] = spouse['int_val']
                unit['intst'] += unit['intsts']
                unit['dbes'] = spouse['div_val']
                unit['dbe'] += unit['dbes']
                unit['alimonys'] = spouse['alm_val']
                unit['alimony'] += unit['alimonys']
                unit['bils'] = spouse['semp_val']
                unit['bil'] += unit['bils']
                unit['pensionss'] = spouse['rtm_val']
                unit['pensions'] += unit['pensionss']
                unit['rentss'] = spouse['rnt_val']
                unit['rents'] += unit['rentss']
                unit['fils'] = spouse['frse_val']
                unit['fil'] += unit['fils']
                unit['ucomps'] = spouse['uc_val']
                unit['ucomp'] += unit['ucomps']
                unit['socsec'] += spouse['ss_val_y']
                # tax unit spouse flag
                spouse['s_flag'] = True

                # CPS evaluation criteria
                unit['zagesp'] = spouse['a_age']
                unit['zworkc'] += spouse['wc_val']
                unit['zsocse'] += spouse['ss_val']
                unit['zssinc'] += spouse['ssi_val']
                unit['zpubas'] += spouse['paw_val']
                unit['zvetbe'] += spouse['vet_val']
                unit['zchsup'] += 0.
                unit['zfinas'] += 0.
                unit['zwassp'] = spouse['wsal_val']

                # blindness indicator
                if spouse['pediseye'] == 1:
                    unit['blind_spouse'] = 1

        unit['xschb'] = 0
        unit['xschf'] = 0
        unit['xsche'] = 0
        unit['xschc'] = 0
        if unit['intst'] > 400.:
            unit['xschb'] = 1
        if unit['fil'] != 0:
            unit['xschf'] = 1
        if unit['rents'] != 0:
            unit['zsche'] = 1
        if unit['bil'] != 0:
            unit['zschc'] = 1

        unit['xxoodep'] = 0
        unit['xxopar'] = 0
        unit['xxtot'] = 0

        unit['hi'] = record['hi']
        unit['paid'] = record['paid']
        unit['priv'] = record['priv']

        # health insurance coverage
        # TODO: This can probably all go. If not, change record to unit
        # record['110'] = 0
        # record['111'] = 0
        # record['112'] = 0
        # record['113'] = np.nan
        # record['114'] = np.nan
        # record['115'] = np.nan
        # if record['sp_ptr'] != 0:
        #     record['113'] = 0
        #     record['114'] = 0
        #     record['115'] = 0
        # # pension coverage
        # record['116'] = 0
        # record['117'] = 0
        # record['118'] = np.nan
        # record['119'] = np.nan
        # if record['sp_ptr'] != 0:
        #     record['118'] = 0
        #     record['119'] = 0
        # # health status
        # record['120'] = 0
        # record['121'] = np.nan
        # if record['sp_ptr'] != 0:
        #     record['121'] = 0
        # # miscellaneous income amounts
        # record['122'] = record['ssi_val']  # SSI
        # record['123'] = record['paw_val']  # public assistance (TANF)
        # record['124'] = record['wc_val']  # workers comp
        # record['125'] = record['vet_val']  # veteran's benefits
        # record['126'] = 0  # child support
        # record['127'] = record['dsab_val']  # disability income
        # record['128'] = record['ss_val']  # social security income
        # record['129'] = record['zowner']
        # record['130'] = 0  # wage share
        # if record['sp_ptr'] != 0:
        #     record['122'] += spouse['ssi_val']
        #     record['123'] += spouse['paw_val']
        #     record['124'] += spouse['wc_val']
        #     record['125'] += spouse['vet_val']
        #     record['126'] = 0
        #     record['127'] += spouse['dsab_val']
        #     record['128'] += spouse['ss_val']
        #     totalwas = record['was']
        #     # Find total wage share
        #     if totalwas > 0:
        #         record['130'] = record['wasp'] / float(totalwas)
        # # Additional health related variables
        # record['135'] = record['ljcw']
        # record['136'] = record['wemind']
        # record['137'] = record['penatvty']
        # record['138'] = np.nan
        # record['139'] = np.nan
        # record['140'] = np.nan
        # record['141'] = np.nan
        # if record['sp_ptr'] != 0:
        #     record['139'] = spouse['ljcw']
        #     record['140'] = spouse['wemind']
        #     record['141'] = spouse['penatvty']
        # # self-employed industry - head and spouse
        # classofworker = record['ljcw']
        # majorindustry = 0
        # senonfarm = 0
        # sefarm = 0
        # if classofworker == 6:
        #     senonfarm = record['semp_val']
        #     sefarm = record['frse_val']
        #     majorindustry = record['wemind']
        # if record['sp_ptr'] != 0:
        #     classofworker = spouse['ljcw']
        #     if classofworker == 6:
        #         senonfarm_sp = spouse['semp_val']
        #         sefarm_sp = spouse['frse_val']
        #         if abs(senonfarm_sp) > abs(senonfarm):
        #             majorindustry = spouse['wemind']
        #             senonfarm += senonfarm_sp
        #             sefarm += sefarm_sp

        # record['146'] = majorindustry
        # record['147'] = senonfarm
        # record['148'] = sefarm

        # # retirement income
        # record['191'] = record['ret_val1']
        # record['192'] = record['ret_sc1']
        # record['193'] = record['ret_val2']
        # record['194'] = record['ret_sc2']
        # record['195'] = np.nan
        # record['196'] = np.nan
        # record['197'] = np.nan
        # record['198'] = np.nan

        # if record['sp_ptr'] != 0:
        #     record['195'] = spouse['ret_val1']
        #     record['196'] = spouse['ret_sc1']
        #     record['197'] = spouse['ret_val2']
        #     record['198'] = spouse['ret_sc2']
        # # disability income

        # record['199'] = record['dis_val1']
        # record['200'] = record['dis_sc1']
        # record['201'] = record['dis_val2']
        # record['202'] = record['dis_sc2']
        # record['203'] = np.nan
        # record['204'] = np.nan
        # record['205'] = np.nan
        # record['206'] = np.nan
        # if record['sp_ptr'] != 0:
        #     record['203'] = spouse['dis_val1']
        #     record['204'] = spouse['dis_sc1']
        #     record['205'] = spouse['dis_val2']
        #     record['206'] = spouse['dis_sc2']

        # # survivor income

        # record['207'] = record['sur_val1']
        # record['208'] = record['sur_sc1']
        # record['209'] = record['sur_val2']
        # record['210'] = record['sur_sc2']
        # record['211'] = np.nan
        # record['212'] = np.nan
        # record['213'] = np.nan
        # record['214'] = np.nan

        # if record['sp_ptr'] != 0:
        #     record['211'] = spouse['sur_val1']
        #     record['212'] = spouse['sur_sc1']
        #     record['213'] = spouse['sur_val2']
        #     record['214'] = spouse['sur_sc2']

        # # veterans income

        # record['215'] = record['vet_typ1']
        # record['216'] = record['vet_typ2']
        # record['217'] = record['vet_typ3']
        # record['218'] = record['vet_typ4']
        # record['219'] = record['vet_typ5']
        # record['220'] = record['vet_val']
        # record['221'] = np.nan
        # record['222'] = np.nan
        # record['223'] = np.nan
        # record['224'] = np.nan
        # record['225'] = np.nan
        # record['226'] = np.nan
        # if record['sp_ptr'] != 0:
        #     record['221'] = spouse['vet_typ1']
        #     record['222'] = spouse['vet_typ2']
        #     record['223'] = spouse['vet_typ3']
        #     record['224'] = spouse['vet_typ4']
        #     record['225'] = spouse['vet_typ5']
        #     record['226'] = spouse['vet_val']
        # # taxpayer
        # record['236'] = record['paw_val']
        # record['237'] = record['mcaid']
        # record['238'] = record['pchip']
        # record['239'] = record['wicyn']
        # record['240'] = record['ssi_val']
        # record['241'] = record['hi_yn']
        # record['242'] = record['hiown']
        # record['243'] = record['hiemp']
        # unit['244'] = record['hipaid']
        # record['245'] = record['emcontrb']
        # unit['246'] = record['hi']
        # record['247'] = record['hityp']
        # record['248'] = record['paid']
        # record['249'] = record['priv']
        # record['250'] = record['prityp']
        # record['251'] = record['ss_val']
        # record['252'] = record['uc_val']
        # record['253'] = record['mcare']
        # record['254'] = record['wc_val']
        # record['255'] = record['vet_val']
        # record['256'] = np.nan
        # record['257'] = np.nan
        # record['258'] = np.nan
        # record['259'] = np.nan
        # record['260'] = np.nan
        # record['261'] = np.nan
        # record['262'] = np.nan
        # record['263'] = np.nan
        # record['264'] = np.nan
        # record['265'] = np.nan
        unit['hi_spouse'] = np.nan
        # record['267'] = np.nan
        unit['paid_spouse'] = np.nan
        unit['priv_spouse'] = np.nan
        # record['270'] = np.nan
        # record['271'] = np.nan
        # record['272'] = np.nan
        # record['273'] = np.nan
        # record['274'] = np.nan
        # record['275'] = np.nan

        if unit['sp_ptr'] != 0:
            # record['256'] = spouse['paw_val']
            # record['257'] = spouse['mcaid']
            # record['258'] = spouse['pchip']
            # record['259'] = spouse['wicyn']
            # record['260'] = spouse['ssi_val']
            # record['261'] = spouse['hi_yn']
            # record['262'] = spouse['hiown']
            # record['263'] = spouse['hiemp']
            # record['264'] = spouse['hipaid']
            # record['265'] = spouse['emcontrb']
            unit['hi_spouse'] = spouse['hi']
            # record['267'] = spouse['hityp']
            unit['paid_spouse'] = spouse['paid']
            unit['priv_spouse'] = spouse['priv']
            # record['270'] = spouse['prityp']
            # record['271'] = spouse['ss_val']
            # record['272'] = spouse['uc_val']
            # record['273'] = spouse['mcare']
            # record['274'] = spouse['wc_val']
            # record['275'] = spouse['vet_val']
        # # Check spouse's age
        # if record['sp_ptr'] != 0:
        #     self.check_age(record, spouse['a_age'])
        # add imputed benefit data
        unit['ssi'] = 0.
        unit['vb'] = 0.
        unit['snap'] = 0.
        unit['mcare'] = 0.
        unit['mcaid'] = 0.
        unit['ss'] = 0.
        unit['tanf'] = 0.
        unit['housing'] = 0.
        unit['wic'] = 0.
        unit['ui'] = 0.
        for var in self.ben_vars:
            var_names = list(var +
                             pd.Series((np.arange(15) + 1).astype(str)))
            for name in var_names:
                unit[name] = 0.
        self.add_benefit(record, unit, 1)
        if unit['sp_ptr'] != 0:
            self.add_benefit(spouse, unit, 2)
        # track where in the benefit recipient position we are
        # spot 2 always held for spouse, even when not present
        unit['ben_number'] = 3

        # search for dependents
        for person in house:
            idxfid = person['ffpos']
            idxhea = person['h_flag']
            idxspo = person['s_flag']
            idxdep = person['d_flag']

            # only determine dependent status if certain conditions are met:
            # 1. a person cannot be a dependent of themselves
            # 2. we only look at immediate family members
            # 3. they cannot already be the head of a tax unit
            # 4. they cannot already be a spouse of a tax unit
            # 5. they cannot already be a dependent of a tax unit
            search = ((house.index(person) != house.index(record)) and
                      idxfid == unit['xfid'] and not idxdep and
                      not idxspo and not idxhea)
            if search:
                person['d_flag'] = self.ifdept(person, unit)
                if person['d_flag']:
                    self.addept(person, unit, house.index(person))

        unit['t_flag'] = True
        return unit

    @staticmethod
    def check_age(record, age, dependent=False):
        """
        Check the age of an individual and adjust age variable accordingly

        Parameters
        ----------
        record: record counting the ages
        age: age of individual
        dependent: True/False indicator for whether or not the person being
                   added is a dependent
        Returns
        -------
        None
        """
        if 0 < age < 18:
            record['nu18'] += 1
            if dependent:
                record['EIC'] += 1
                record['nu18_dep'] += 1
                if age <= 5:
                    record['nu05'] += 1
                elif age <= 13:
                    record['nu13'] += 1
                    record['f2441'] += 1
                if 0 < age <= 17:
                    record['n24'] += 1
        elif 18 <= age <= 20:
            record['n1820'] += 1
        elif age >= 21:
            record['n21'] += 1
            if age >= 65 and dependent:
                record['elderly_dependent'] += 1

    @staticmethod
    def totincx(unit):
        """
        Calculate total income for the unit
        Parameters
        ----------
        unit: unit income is being calculated for

        Returns
        -------
        total income
        """
        totinc = (unit['was'] + unit['intst'] + unit['dbe'] + unit['alimony'] +
                  unit['bil'] + unit['pensions'] + unit['rents'] +
                  unit['fil'] + unit['ucomp'] + unit['socsec'])
        return totinc

    @staticmethod
    def relation(person, record):
        """
        Determine relationship between subfamilies

        Parameters
        -----------
        person: individual being checked
        record: record they may be related to

        Returns
        -------
        Code for related
        """
        ref_person = record['relcode']
        index_person = person['a_exprrp']
        if ref_person == 5:
            genref = -1
        elif ref_person == 7:
            genref = -2
        elif ref_person == 8:
            genref = 1
        elif ref_person == 9:
            genref = 0
        elif ref_person == 11:
            genref = -1
        else:
            genref = 99

        if index_person == 5:
            genind = -1
        elif index_person == 7:
            genind = -2
        elif index_person == 8:
            genind = 1
        elif index_person == 9:
            genind = 0
        elif index_person == 11:
            genind = -1
        else:
            genind = 99

        if genref != 99 and genind != 99:
            related = genind - genref
        else:
            related = 99
        return related

    @staticmethod
    def add_benefit(person, unit, pos):
        """
        Add imputed benefit value of an individual to a tax unit
        Parameters
        ----------
        person: person benefits are being counted for
        unit: main tax unit
        pos: person's position in the tax unit

        Returns
        -------
        None
        """
        # tuple format:
        # a - name used in the actual record
        # b - imputed value name in the raw CPS
        # c - imputed probability in the raw CPS
        # d - capitalized name used in the extrapolation routine
        ben_list = [('ssi', 'ssi_impute', 'ssi_probs', 'SSI'),
                    ('vb', 'vb_impute', 'vb_probs', 'VB'),
                    ('snap', 'snap_impute', 'snap_probs', 'SNAP'),
                    ('mcare', 'MedicareX', 'mcare_probs', 'MCARE'),
                    ('mcaid', 'MedicaidX', 'mcaid_probs', 'MCAID'),
                    ('ss', 'ss_val_y', 'ss_probs', 'SS'),
                    ('tanf', 'tanf_impute', 'tanf_probs', 'TANF'),
                    ('ui', 'ui_impute', 'ui_probs', 'UI'),
                    ('housing', 'housing_impute', 'housing_probs', 'HOUSING'),
                    ('wic', 'wic_impute', 'wic_probs', 'WIC')]
        for a, b, c, d in ben_list:
            unit[a] += person[b]
            unit['{}_PROB{}'.format(d, pos)] = person[c]
            unit['{}_VAL{}'.format(d, pos)] = person[b]

    def hhstatus(self, unit):
        """
        Determine head of household filing status

        Parameters
        ----------
        unit: tax unit to be checked

        Returns
        -------
        None
        """
        income = 0.
        # find total income for the tax unit
        for unit in self.house_units:
            income += self.totincx(unit)
        # total unit for the individual
        if income > 0:
            totinc = self.totincx(unit)
            # TODO: check HOH filing requirements
            if unit['js'] == 1 and float(totinc) / income > 0.99:
                if unit['ifdept'] != 1 and unit['depne'] > 0:
                    unit['js'] = 3

    def must_file(self, record):
        """
        Determine if a dependent must file a return
        Parameters
        ----------
        record: record for the dependent
        Returns
        -------
        True/False
        """
        wages = record['wsal_val']
        income = (wages + record['semp_val'] + record['frse_val'] +
                  record['uc_val'] + record['ss_val'] + record['rtm_val'] +
                  record['int_val'] + record['div_val'] + record['rnt_val'] +
                  record['alm_val'])
        # unit must file if their wages or income exceed a threshold
        depfile = wages > self.depwages or income > self.depTotal
        return depfile

    def convert(self, ix, iy):
        """
        Convert an existing tax unit (ix) into a dependent filer of (iy)

        Parameters
        ----------
        ix: index location of tax unit to be converted
        iy: index location of targeted tax unit

        Returns
        -------
        None
        """
        self.house_units[ix]['ifdept'] = True
        ixdeps = self.house_units[ix]['depne']
        iydeps = self.house_units[iy]['depne']
        self.house_units[ix]['depne'] = 0
        ixjs = self.house_units[ix]['js']
        if ixjs == 2:
            self.house_units[iy]['depne'] += ixdeps + 2
            # Add location of two new dependets
            self.house_units[iy]['dep' + str(iydeps + 1)] = ix
            self.house_units[iy][('dep' +
                                  str(iydeps +
                                      2))] = self.house_units[ix]['sp_ptr']
            # Add ages of two new dependents
            self.house_units[iy][('depage' +
                                  str(iydeps +
                                      1))] = self.house_units[ix]['ageh']
            self.house_units[iy][('depage' +
                                  str(iydeps +
                                      2))] = self.house_units[ix]['ages']
            iybgin = iydeps + 2
        else:
            self.house_units[iy]['depne'] += ixdeps + 1
            self.house_units[iy]['dep' + str(iydeps + 1)] = ix
            self.house_units[iy][('depage' +
                                  str(iydeps +
                                      1))] = self.house_units[ix]['ageh']
            iybgin = iydeps + 1
        if ixdeps > 0:
            # Assign any dependents to target record
            for ndeps in range(1, ixdeps + 1):
                dep = 'dep' + str(iybgin + ndeps)
                depx = 'dep' + str(ndeps)
                depage = 'depage' + str(iybgin + ndeps)
                depagex = 'depage' + str(ndeps)
                self.house_units[iy][dep] = self.house_units[ix][depx]
                self.house_units[ix][dep] = 0
                self.house_units[iy][depage] = self.house_units[ix][depagex]
        # Add age variables together
        self.house_units[iy]['nu05'] += self.house_units[ix]['nu05']
        self.house_units[iy]['nu13'] += self.house_units[ix]['nu13']
        self.house_units[iy]['nu18_dep'] += self.house_units[ix]['nu18_dep']
        self.house_units[iy]['nu18'] += self.house_units[ix]['nu18']
        self.house_units[iy]['n1820'] += self.house_units[ix]['n1820']
        self.house_units[iy]['n21'] += self.house_units[ix]['n21']
        elderly = self.house_units[ix]['elderly_dependent']
        self.house_units[iy]['elderly_dependent'] += elderly
        self.house_units[iy]['n24'] += self.house_units[ix]['n24']
        self.house_units[iy]['EIC'] += self.house_units[ix]['EIC']
        self.house_units[iy]['f2441'] += self.house_units[ix]['f2441']
        # TODO: add dependent info
        # transfer benefit totals
        self.house_units[iy]['ssi'] += self.house_units[ix]['ssi']
        self.house_units[iy]['vb'] += self.house_units[ix]['vb']
        self.house_units[iy]['snap'] += self.house_units[ix]['snap']
        self.house_units[iy]['mcare'] += self.house_units[ix]['mcare']
        self.house_units[iy]['mcaid'] += self.house_units[ix]['mcaid']
        self.house_units[iy]['ss'] += self.house_units[ix]['ss']
        self.house_units[iy]['tanf'] += self.house_units[ix]['tanf']
        self.house_units[iy]['ui'] += self.house_units[ix]['ui']
        self.house_units[iy]['housing'] += self.house_units[ix]['housing']
        self.house_units[iy]['wic'] += self.house_units[ix]['wic']

        # transfer benefit probabilities and values
        ben_list = ['SSI', 'SNAP', 'VB', 'MCARE', 'MCAID', 'SS', 'TANF',
                    'UI', 'HOUSING', 'WIC']
        for i in range(1, self.house_units[ix]['ben_number']):
            for ben in ben_list:
                # current position in the benefits probability/value count
                pos = self.house_units[iy]['ben_number']
                prob_str1 = '{}_PROB{}'.format(ben, pos)
                prob_str2 = '{}_PROB{}'.format(ben, i)
                new_prob = self.house_units[ix][prob_str2]
                self.house_units[iy][prob_str1] = new_prob
                val_str1 = '{}_VAL{}'.format(ben, pos)
                val_str2 = '{}_VAL{}'.format(ben, i)
                new_val = self.house_units[ix][val_str2]
                self.house_units[iy][val_str1] = new_val
            # increment benefit position
            self.house_units[iy]['ben_number'] += 1

    def ifdept(self, person, record):
        """
        Determine if an individual is a dependent of the reference person
        Five tests must be passed for an individual to be a dependent:
            1. Relationship
            2. Marital status
            3. Citizenship
            4. Income
            5. Support
        Parameters
        ----------
        person: individual being evaluated
        record: reference person

        Returns
        -------
        True if person is a dependent, false otherwise
        """
        test1 = 1  # Only looking at families so assume test is passed
        test2 = 1  # Only looking at families so assume test is passed
        test3 = 1  # Assume citizenship requirment is always met
        test4 = 0
        test5 = 0
        dflag = False
        age = person['a_age']
        income = (person['wsal_val'] + person['semp_val'] +
                  person['frse_val'] + person['uc_val'] + person['ss_val_y'] +
                  person['rtm_val'] + person['int_val'] + person['int_val'] +
                  person['div_val'] + person['rnt_val'] + person['alm_val'])
        related = self.relation(person, record)
        # test 4: income test
        if income <= 2500.:
            test4 = 1
        if person['a_exprrp'] == 5 or related == -1:  # should it be person?
            if age <= 18 or (age <= 23 and person['a_enrlw'] > 0):
                test4 = 1

        # test 5: support test
        totinc = self.totincx(record)
        if totinc + income > 0:
            if income / float(totinc + income) < 0.5:
                test5 = 1
        else:
            test5 = 1

        dtest = test1 + test2 + test3 + test4 + test5
        if dtest == 5:
            dflag = True
        return dflag

    def addept(self, person, record, p_index):
        """
        Parameters
        ----------
        person: individual beng claimed as a dependent
        record: reference person
        p_index: index of person being claimed

        Returns
        -------
        None
        """
        person['d_flag'] = True  # flag as dependent
        record['depne'] += 1
        depne = record['depne']
        record['dep' + str(depne)] = p_index
        # add age of dependent to age variables
        self.check_age(record, person['a_age'], True)
        record['depage' + str(depne)] = person['a_age']
        # add benefit variables to the record
        self.add_benefit(person, record, record['ben_number'])
        record['ben_number'] += 1

    def filst(self, record):
        """
        Determines if a tax unit files a return using five tests:
            1. Wage test: If anyone in the unit had wage and salary income, the
                          unit is deemed to be a filer
            2. Gross income test. The income thresholds in the 1040 filing
               requirements are used to determine if the tax unit has to file.
            3. Dependent filer test. Individuals who are claimed as dependents,
               but are required to file a return
            4. Random selection
            5. Negative income

        Parameters
        ----------
        record: a tax unit

        Returns
        -------
        filing status: 1 if filer; 0 otherwise
        """
        dep_exemption = record['depne'] * self.depExempt
        # wages and gross income tests
        income = (record['was'] + record['intst'] + record['dbe'] +
                  record['alimony'] + record['bil'] + record['pensions'] +
                  record['rents'] + record['fil'] + record['ucomp'])
        # single filers
        if record['js'] == 1:
            if record['was'] >= self.wage1:  # wage test
                return 1
            # income test
            amount = self.single - dep_exemption
            if record['agede'] != 0:
                amount = self.single65 - dep_exemption
            if income >= amount:
                return 1

        elif record['js'] == 2:  # joint filers
            # wage test depends on number of dependents
            if record['depne'] > 0:
                if record['was'] >= self.wage2:
                    return 1
            else:
                if record['was'] >= self.wage2nk:
                    return 1
            # income tests
            amount = self.joint - dep_exemption
            if record['agede'] == 1:
                amount = self.joint65one - dep_exemption
            elif record['agede'] == 2:
                amount = self.joint65both - dep_exemption
            if income >= amount:
                return 1

        elif record['js'] == 3:  # head of household
            if record['was'] >= self.wage3:
                return 1
            # income test
            amount = self.hoh  # should this subtract dep_exemption?
            if record['agede'] != 0:
                amount = self.hoh65 - dep_exemption
            if income >= amount:
                return 1

        if record['ifdept']:  # dependent filer test
            return 1

        # random selection
        fils = (record['js'] == 3 and record['agede'] > 0 and
                income > 6500. and record['depne'] > 0)
        if fils:
            return 0

        # negative income test
        if record['bil'] < 0 or record['fil'] < 0 or record['rents'] < 0:
            return 1

        return 0

    def search(self):
        """
        search for dependencies among tax units
        Parameters
        ----------
        None
        Returns
        -------
        None
        """
        highest = -9.9e32
        idxhigh = 0
        for unit in self.house_units:
            totinc = self.totincx(unit)
            if totinc >= highest:
                highest = totinc
                idxhigh = self.house_units.index(unit)
        # search only units that aren't already dependents
        if not self.house_units[idxhigh]['ifdept']:
            for ix in range(0, self.nunits):
                unit = self.house_units[ix]
                idxjs = unit['js']
                idxdepf = unit['ifdept']
                idxrelc = unit['relcode']
                idxfamt = unit['ftype']

                convert1 = (ix != idxhigh and idxdepf != 1 and
                            highest > 0. and idxjs != 2)
                if convert1:
                    if idxfamt == 1 or idxfamt == 3 or idxfamt == 5:
                        totinc = self.totincx(unit)
                        if totinc <= 0.:
                            unit['t_flag'] = False  # no longer a tax unit
                            self.convert(ix, idxhigh)
                        if 0. < totinc <= 3000.:
                            unit['t_flag'] = False
                            self.convert(ix, idxhigh)
                    if idxrelc == 11:
                        unit['t_flag'] = False
                        self.convert(ix, idxhigh)

    def output(self, unit, house):
        """
        After the tax units have been created, format for output
        Parameters
        ----------
        unit: head of tax unit
        house: household of tax unit
        Returns
        -------
        Completed tax unit
        """
        record = {}
        depne = unit['depne']
        # all of these benefits are already in the unit and will be kept
        repeated_vars = ['js', 'ifdept', 'agede', 'depne', 'ageh',
                         'ages', 'was', 'intst', 'dbe', 'alimony', 'bil',
                         'pensions', 'rents', 'fil', 'ucomp', 'socsec',
                         'wt', 'zifdep', 'zntdep', 'zhhinc', 'zagept',
                         'zagesp', 'zoldes', 'zyoung', 'zworkc', 'zsocse',
                         'zssinc', 'zpubas', 'zvetbe', 'zchsup', 'zdepin',
                         'zowner', 'zwaspt', 'zwassp', 'wasp', 'wass', 'nu18',
                         'n1820', 'n21', 'ssi', 'ss', 'snap', 'vb', 'mcare',
                         'mcaid', 'tanf', 'housing', 'wic', 'ui', 'xstate',
                         'xregion', 'xschf', 'xschb',
                         'xsche', 'xschc', 'xhid', 'xfid', 'xpid',
                         'intstp', 'intsts', 'dbep', 'dbes', 'alimonyp',
                         'alimonys', 'pensionsp', 'pensionss', 'rentsp',
                         'rentss', 'filp', 'fils', 'bilp', 'bils', 'hi',
                         'paid', 'priv', 'xhid', 'xpid', 'xfid', 'xstate',
                         'a_lineno', 'n24', 'nu18', 'n1820', 'n21', 'nu05',
                         'nu13', 'f2441', 'elderly_dependent', 'nu18_dep',
                         'EIC', 'agi', 'agi_head', 'agi_spouse', 'blind_head',
                         'blind_spouse', 'year', 'hi_spouse', 'paid_spouse',
                         'priv_spouse']
        # additional benefit variables
        for var in self.ben_vars:
            var_names = list(var +
                             pd.Series((np.arange(15) + 1).astype(str)))
            repeated_vars += var_names
        for var in repeated_vars:
            record[var] = unit[var]

        txpye = 1
        if unit['js'] == 1:
            txpye = 2
        record['xxtot'] = txpye + depne
        # ensure XTOT will equal the number of people in a tax unit
        nsums = record['nu18'] + record['n1820'] + record['n21']
        eflag = False
        if record['xxtot'] != nsums or record['n24'] > record['nu18']:
            eflag = True
            record['xxtot'] = 1
            record['nu18'] = 0
            record['n1820'] = 0
            record['n21'] = 0
            record['n24'] = 0
            record['EIC'] = 0.
            record['f2441'] = 0.
            record['elderly_dependent'] - 0.
            record['nu18_dep'] = 0.
            record['nu05'] = 0.
            record['nu13'] = 0.
            self.check_age(record, record['ageh'])
            if record['js'] == 2:
                record['xxtot'] += 1
                self.check_age(record, record['ages'])

        # check relationship codes among dependents
        xxoodep = 0
        xxopar = 0
        xxocah = 0
        record['xxocawh'] = 0
        if depne > 0:
            for i in range(1, depne + 1):
                dindex = unit['dep' + str(i)]
                drel = house[dindex]['a_exprrp']
                dage = house[dindex]['a_age']
                # check ages again if XTOT error was found
                if eflag:
                    record['xxtot'] += 1
                    self.check_age(record, dage, dependent=True)
                if drel == 8:
                    xxopar += 1
                if drel >= 9 and dage >= 18:
                    xxoodep += 1
                if dage < 18:
                    xxocah += 1

        oldest = 0
        youngest = 0
        if depne > 0:
            oldest = -9.9e16
            youngest = 9.9e16
            for i in range(1, depne + 1):
                dage = unit['depage' + str(i)]
                if dage > oldest:
                    oldest = dage
                if dage < youngest:
                    youngest = dage
            record['zoldes'] = oldest
            record['zyoung'] = youngest
        record['oldest'] = oldest
        record['youngest'] = youngest

        # dependent income
        zdepin = 0.
        if depne > 0:
            for i in range(1, depne + 1):
                dindex = unit['dep' + str(i)]
                if not house[dindex]['flag']:
                    zdepin = (house[dindex]['wsal_val'] +
                              house[dindex]['semp_val'] +
                              house[dindex]['frse_val'] +
                              house[dindex]['uc_val'] +
                              house[dindex]['ss_val'] +
                              house[dindex]['semp_val'] +
                              house[dindex]['rtm_val'] +
                              house[dindex]['int_val'] +
                              house[dindex]['div_val'] +
                              house[dindex]['alm_val'])
        record['zdepin'] = zdepin
        record['income'] = self.totincx(unit)
        record['filst'] = self.filst(unit)

        return record
