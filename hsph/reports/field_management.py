from collections import defaultdict
import datetime
import restkit.errors
import time

from django.utils.datastructures import SortedDict

from dimagi.utils.couch.database import get_db
from dimagi.utils.decorators.memoized import memoized

from corehq.apps.fixtures.models import FixtureDataType, FixtureDataItem
from corehq.apps.reports.standard import ProjectReportParametersMixin, DatespanMixin, CustomProjectReport
from corehq.apps.reports.standard.inspect import CaseDisplay, CaseListReport
from corehq.apps.reports.datatables import DataTablesColumn, DataTablesHeader
from corehq.apps.reports.generic import GenericTabularReport
from hsph.reports import HSPHSiteDataMixin
from hsph.fields import AllocatedToFilter
from corehq.apps.api.es import FullCaseES

def short_date_format(date):
    return date.strftime('%d-%b')

def datestring_minus_days(datestring, days):
    date = datetime.datetime.strptime(datestring[:10], '%Y-%m-%d')
    return (date - datetime.timedelta(days=days)).isoformat()

def get_user_site_map(domain):
    user_site_map = defaultdict(list)
    data_type = FixtureDataType.by_domain_tag(domain, 'site').first()
    fixtures = FixtureDataItem.by_data_type(domain, data_type.get_id)
    for fixture in fixtures:
        for user in fixture.get_users():
            user_site_map[user._id].append(fixture.fields['site_id'])
    return user_site_map


class FIDAPerformanceReport(GenericTabularReport, CustomProjectReport,
                            ProjectReportParametersMixin, DatespanMixin):
    """
    BetterBirth Shared Dropbox/Updated ICT package/Reporting Specs/FIDA Performance_v2.xls 
    """
    name = "FIDA Performance Report"
    slug = "hsph_fida_performance"
    
    fields = ['corehq.apps.reports.fields.FilterUsersField',
              'corehq.apps.reports.fields.DatespanField',
              'hsph.fields.NameOfFIDAField']

    filter_group_name = "Role - FIDA" 

    @property
    def headers(self):
        return DataTablesHeader(
            DataTablesColumn("Name of FIDA"),
            #DataTablesColumn("Name of Team Leader"),
            DataTablesColumn("No. of Facilities Covered"),
            DataTablesColumn("No. of Facility Visits"),
            DataTablesColumn("No. of Facilities with less than 2 visits/week"),
            DataTablesColumn("Average Time per Birth Record"),
            DataTablesColumn("Average Number of Birth Records Uploaded Per Visit"),
            DataTablesColumn("No. of Births with no phone details"),
            DataTablesColumn("No. of Births with no address"),
            DataTablesColumn("No. of Births with no contact info"),
            DataTablesColumn("No. of Home Visits assigned"),
            DataTablesColumn("No. of Home Visits completed"),
            DataTablesColumn("No. of Home Visits completed per day"),
            DataTablesColumn("No. of Home Visits Open at 30 Days"))

    @property
    def rows(self):
        user_site_map = get_user_site_map(self.domain)

        # ordered keys with default values
        keys = SortedDict([
            ('fidaName', None),
            #('teamLeaderName', None),
            ('facilitiesCovered', 0),
            ('facilityVisits', 0),
            ('facilitiesVisitedLessThanTwicePerWeek', None),
            ('avgBirthRegistrationTime', None),
            ('birthRegistrationsPerVisit', None),
            ('noPhoneDetails', 0),
            ('noAddress', 0),
            ('noContactInfo', 0),
            ('homeVisitsAssigned', 0),
            ('homeVisitsCompleted', 0),
            ('homeVisitsCompletedPerDay', 0),
            ('homeVisitsOpenAt30Days', 0)
        ])

        rows = []
        db = get_db()

        startdate = self.datespan.startdate_param_utc[:10]
        enddate = self.datespan.enddate_param_utc[:10]
        
        to_date = lambda string: datetime.datetime.strptime(
                        string, "%Y-%m-%d").date()
        weeks = (to_date(enddate) - to_date(startdate)).days // 7

        for user in self.users:
            user_id = user.get('user_id')

            row = db.view('hsph/fida_performance',
                startkey=["all", self.domain, user_id, startdate],
                endkey=["all", self.domain, user_id, enddate],
                reduce=True,
                wrapper=lambda r: r['value']
            ).first() or {}

            workingDays = db.view('hsph/fida_performance',
                startkey=["workingDays", self.domain, user_id, startdate],
                endkey=["workingDays", self.domain, user_id, enddate],
                reduce=False,
                wrapper=lambda r: r['value']['workingDay']).all()
            workingDays = set(workingDays)

            row['fidaName'] = self.table_cell(
                    user.get('raw_username'), user.get('username_in_report'))
            row['facilitiesCovered'] = len(user_site_map[user_id])
            row['facilitiesVisitedLessThanTwicePerWeek'] = len(
                filter(
                    lambda count: count < weeks * 2, 
                    [row.get(site_id + 'Visits', 0) 
                     for site_id in user_site_map[user_id]]
                )
            )
            if row.get('avgBirthRegistrationTime'):
                row['avgBirthRegistrationTime'] = time.strftime(
                        '%M:%S', time.gmtime(row['avgBirthRegistrationTime']))
            else:
                row['avgBirthRegistrationTime'] = None

            if workingDays:
                row['homeVisitsCompletedPerDay'] = round(
                        row.get('homeVisitsCompleted', 0) / float(len(workingDays)), 1)
            else:
                row['homeVisitsCompletedPerDay'] = 0.0

            # These queries can fail if startdate is less than N days before
            # enddate.  We just catch and supply a default value.
            try:
                row['homeVisitsAssigned'] = db.view('hsph/fida_performance',
                    startkey=['assigned', self.domain, user_id, startdate],
                    endkey=['assigned', self.domain, user_id,
                        datestring_minus_days(enddate, 21)],
                    reduce=True,
                    wrapper=lambda r: r['value']['homeVisitsAssigned']
                ).first()
            except restkit.errors.RequestFailed:
                row['homeVisitsAssigned'] = 0

            try:
                row['homeVisitsOpenAt30Days'] = db.view('hsph/fida_performance',
                    startkey=['open30Days', self.domain, user_id, startdate],
                    endkey=['open30Days', self.domain, user_id,
                        datestring_minus_days(enddate, 29)],
                    reduce=True,
                    wrapper=lambda r: r['value']['homeVisitsOpenAt30Days']
                ).first()
            except restkit.errors.RequestFailed:
                row['homeVisitsOpenAt30Days'] = 0

            list_row = []
            for k, v in keys.items():
                val = row.get(k, v)
                if val is None:
                    val = '---'
                list_row.append(val)

            rows.append(list_row)

        return rows


class HSPHCaseDisplay(CaseDisplay):

    @property
    @memoized
    def _date_admission(self):
        return self.parse_date(self.case['date_admission'])

    @property
    def region(self):
        try:
            return self.report.get_region_name(self.case['region_id'])
        except AttributeError:
            return ""

    @property
    def district(self):
        try:
            return self.report.get_district_name(
                self.case['region_id'], self.case['district_id'])
        except AttributeError:
            return ""

    @property
    def site(self):
        try:
            return self.report.get_site_name(
                self.case['region_id'], self.case['district_id'],
                self.case['site_number'])
        except AttributeError:
            return ""

    @property
    def patient_id(self):
        return self.case.get('patient_id', '')

    @property
    def status(self):
        return "Closed" if self.case['closed'] else "Open"

    @property
    def mother_name(self):
        return self.case.get('name_mother', '')

    @property
    def date_admission(self):
        return short_date_format(self._date_admission)

    @property
    def address(self):
        return self.case.get('house_address', '')

    @property
    @memoized
    def allocated_to(self):
        # this logic is duplicated for elasticsearch in CaseReport.case_filter
        UNKNOWN = "Unknown"
        CALL_CENTER = "Call Center"
        FIELD = "Field"

        if self.case['closed']:
            if 'closed_by' not in self.case:
                return UNKNOWN

            if self.case['closed_by'] in ("cati", "cati_tl"):
                return CALL_CENTER
            elif self.case['closed_by'] in ("fida", "field_manager"):
                return FIELD
            else:
                return UNKNOWN
        else:
            today = datetime.datetime.now()
            if today <= self._date_admission + datetime.timedelta(days=21):
                return CALL_CENTER
            else:
                return FIELD
    
    @property
    def allocated_start(self):
        try:
            delta = datetime.timedelta(
                    days=8 if self.allocated_to == "Call Center" else 21)
            return short_date_format(self._date_admission + delta)
        except AttributeError:
            return ""

    @property
    def allocated_end(self):
        try:
            delta = datetime.timedelta(
                    days=20 if self.allocated_to == 'Call Center' else 29)
            return short_date_format(self._date_admission + delta)
        except AttributeError:
            return ""

    @property
    def outside_allocated_period(self):
        if self.case['closed_on']:
            compare_date = self.parse_date(
                    self.case['closed_on']).replace(tzinfo=None)
        else:
            compare_date = datetime.datetime.utcnow().replace(tzinfo=None)

        return 'Yes' if (compare_date - self._date_admission).days > 29 else 'No'


class CaseReport(CaseListReport, CustomProjectReport, HSPHSiteDataMixin,
                 DatespanMixin):
    name = 'Case Report'
    slug = 'case_report'
    
    fields = (
        'corehq.apps.reports.fields.FilterUsersField',
        'corehq.apps.reports.fields.DatespanField',
        'hsph.fields.SiteField',
        'hsph.fields.AllocatedToFilter',
        'hsph.fields.NameOfFIDAField',
        'corehq.apps.reports.fields.SelectOpenCloseField',
    )

    default_case_type = 'birth'

    @property
    @memoized
    def case_es(self):
        return FullCaseES(self.domain)

    @property
    def headers(self):
        headers = DataTablesHeader(
            DataTablesColumn("Region"),
            DataTablesColumn("District"),
            DataTablesColumn("Site"),
            DataTablesColumn("Patient ID"),
            DataTablesColumn("Status"),
            DataTablesColumn("Mother Name"),
            DataTablesColumn("Date of Admission"),
            DataTablesColumn("Address of Patient"),
            DataTablesColumn("Allocated To"),
            DataTablesColumn("Allocated Start"),
            DataTablesColumn("Allocated End"),
            DataTablesColumn("Outside Allocated Period")
        )
        headers.no_sort = True
        return headers

    @property
    def case_filter(self):
        allocated_to = self.request_params.get(AllocatedToFilter.slug, '')
        region_id = self.request_params.get('hsph_region', '')
        district_id = self.request_params.get('hsph_district', '')
        site_num = str(self.request_params.get('hsph_site', ''))

        filters = [{
            'range': {
                'opened_on': {
                    "from": self.datespan.startdate_param_utc,
                    "to": self.datespan.enddate_param_utc
                }
            }
        }]
        
        if site_num:
            filters.append({'term': {'site_number': site_num.lower()}})
        if district_id:
            filters.append({'term': {'district_id': district_id.lower()}})
        if region_id:
            filters.append({'term': {'region_id': region_id.lower()}})

        if allocated_to:
            max_date_admission = (datetime.date.today() -
                datetime.timedelta(days=21)).strftime("%Y-%m-%d")

            call_center_filter = {
                'or': [
                    {'and': [
                        {'term': {'closed': True}},
                        {'prefix': {'closed_by': 'cati'}}
                    ]},
                    {'and': [
                        {'term': {'closed': False}},
                        {'range': {
                            'date_admission': {
                                'from': max_date_admission
                            }
                        }}
                    ]}
                ]
            }

            if allocated_to == 'cati':
                filters.append(call_center_filter)
            else:
                filters.append({'not': call_center_filter})

        return {'and': filters} if filters else {}

    @property
    def shared_pagination_GET_params(self):
        params = super(CaseReport, self).shared_pagination_GET_params

        slugs = [
            AllocatedToFilter.slug,
            'hsph_region',
            'hsph_district',
            'hsph_site',
            'startdate',
            'enddate'
        ]

        for slug in slugs:
            params.append({
                'name': slug,
                'value': self.request_params.get(slug, '')
            })

        return params

    @property
    def rows(self):
        case_displays = (HSPHCaseDisplay(self, self.get_case(case))
                         for case in self.es_results['hits'].get('hits', []))

        for disp in case_displays:
            yield [
                disp.region,
                disp.district,
                disp.site,
                disp.patient_id,
                disp.status,
                disp.case_link,
                disp.date_admission,
                disp.address,
                disp.allocated_to,
                disp.allocated_start,
                disp.allocated_end,
                disp.outside_allocated_period,
            ]
