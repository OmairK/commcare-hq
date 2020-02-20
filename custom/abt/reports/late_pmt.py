from collections import defaultdict, namedtuple
from typing import List

from django.db.models.aggregates import Count
from django.utils.functional import cached_property
from django.utils.translation import ugettext as _

from dateutil.rrule import DAILY, FR, MO, SA, TH, TU, WE, rrule
from sqlagg.columns import SimpleColumn
from sqlagg.filters import EQ
from sqlagg.sorting import OrderBy

from corehq.apps.fixtures.dbaccessors import (
    get_fixture_data_types_in_domain,
    get_fixture_items_for_data_type,
)
from corehq.apps.fixtures.models import FixtureDataItem
from corehq.apps.reports.datatables import DataTablesColumn, DataTablesHeader
from corehq.apps.reports.filters.dates import DatespanFilter
from corehq.apps.reports.generic import GenericTabularReport
from corehq.apps.reports.sqlreport import DatabaseColumn, SqlData
from corehq.apps.reports.standard import CustomProjectReport, DatespanMixin
from corehq.apps.sms.models import INCOMING, OUTGOING, SMS
from corehq.apps.userreports.util import get_table_name
from corehq.sql_db.connections import DEFAULT_ENGINE_ID
from custom.abt.reports.filters import (
    CountryFilter,
    LevelFourFilter,
    LevelOneFilter,
    LevelThreeFilter,
    LevelTwoFilter,
    SubmissionStatusFilter,
    UsernameFilter,
    LocationFilter)

LocationTuple = namedtuple('LocationTuple', [
    'id',
    'name',
    'country',
    'level_1',
    'level_2',
    'level_3',
    'level_4',
])


class LatePmtReport(GenericTabularReport, CustomProjectReport, DatespanMixin):
    report_title = "Late PMT"
    slug = 'late_pmt'
    name = "Late PMT"

    languages = (
        'en',
        'fra',
        'por'
    )

    fields = [
        DatespanFilter,
        LocationFilter,
        CountryFilter,
        LevelOneFilter,
        LevelTwoFilter,
        LevelThreeFilter,
        LevelFourFilter,
        SubmissionStatusFilter,
    ]

    @property
    def report_config(self):
        return {
            'domain': self.domain,
            'startdate': self.startdate,
            'enddate': self.enddate,
            'location_id': self.request.GET.get('location_id', ''),
            'country': self.request.GET.get('country', ''),
            'level_1': self.request.GET.get('level_1', ''),
            'level_2': self.request.GET.get('level_2', ''),
            'level_3': self.request.GET.get('level_3', ''),
            'level_4': self.request.GET.get('level_4', ''),
            'submission_status': self.request.GET.get('submission_status', '')
        }

    @property
    def startdate(self):
        return self.request.datespan.startdate

    @property
    def enddate(self):
        return self.request.datespan.end_of_end_day

    @property
    def headers(self):
        return DataTablesHeader(
            DataTablesColumn(_("Missing Report Date")),
            DataTablesColumn(_("Name")),
            DataTablesColumn(_("Country")),
            DataTablesColumn(_("Level 1")),
            DataTablesColumn(_("Level 2")),
            DataTablesColumn(_("Level 3")),
            DataTablesColumn(_("Level 4")),
            DataTablesColumn(_("Submission Status")),
        )

    @cached_property
    def smss_received(self):
        # TODO: Associate SMSes with locations, not users
        data = SMS.objects.filter(
            domain=self.domain,
            couch_recipient_doc_type='CommCareUser',
            direction=INCOMING,
            couch_recipient__in=list(get_locations(self.domain).keys()),
            date__range=(
                self.startdate,
                self.enddate
            )
        ).exclude(
            text="123"
        ).values('date', 'couch_recipient').annotate(
            number_of_sms=Count('couch_recipient')
        )
        return {(sms['date'].date(), sms['couch_recipient']) for sms in data}

    @property
    def rows(self):
        def _to_report_format(date, location, error_msg):
            return [
                date.strftime("%Y-%m-%d"),
                location.name,
                location.country,
                location.level_1,
                location.level_2,
                location.level_3,
                location.level_4,
                error_msg
            ]

        include_missing_pmt_data = self.report_config['submission_status'] != 'group_b'
        # include_incorrect_pmt_data is no longer applicable, because invalid
        # SMSs can't reach HQ any more.
        include_incorrect_pmt_data = self.report_config['submission_status'] != 'group_a'
        dates = rrule(
            DAILY,
            dtstart=self.startdate,
            until=self.enddate,
            byweekday=(MO, TU, WE, TH, FR, SA)
        )
        rows = []
        for date in dates:
            for location in self.locations:
                # TODO: Ensure that sms['couch_recipient'] is / can be mapped to location.id
                sms_received = (date.date(), location.id) in self.smss_received
                if not sms_received and (include_missing_pmt_data or include_incorrect_pmt_data):
                    error_msg = _('Incorrect or no PMT data submitted')
                    rows.append(_to_report_format(date, location, error_msg))
        return rows

    @cached_property
    def locations(self) -> List[LocationTuple]:
        data_types_by_tag = {
            dt.tag: dt
            for dt in get_fixture_data_types_in_domain(self.domain)
        }
        level_1_items = get_fixture_items_for_data_type(
            self.domain,
            data_types_by_tag["level_1_dcv"]._id
        )
        level_1_dicts = [fixture_data_item_to_dict(di) for di in level_1_items]
        level_2s_by_level_1 = get_fixture_dicts_by_key(
            self.domain,
            data_type_id=data_types_by_tag["level_2_dcv"]._id,
            key='level_1_dcv'
        )
        level_3s_by_level_2 = get_fixture_dicts_by_key(
            self.domain,
            data_type_id=data_types_by_tag["level_3_dcv"]._id,
            key='level_2_dcv'
        )
        level_4s_by_level_3 = get_fixture_dicts_by_key(
            self.domain,
            data_type_id=data_types_by_tag["level_4_dcv"]._id,
            key='level_3_dcv'
        )
        country_has_level_4 = len(level_4s_by_level_3) > 1

        locations = []
        for level_1 in level_1_dicts:
            for level_2 in level_2s_by_level_1[level_1['id']]:
                for level_3 in level_3s_by_level_2[level_2['id']]:
                    if country_has_level_4:
                        for level_4 in level_4s_by_level_3[level_3['id']]:
                            locations.append(LocationTuple(
                                id=level_4['id'],
                                name=level_4['name'],
                                country=level_1['country'],
                                level_1=level_1['name'],
                                level_2=level_2['name'],
                                level_3=level_3['name'],
                                level_4=level_4['name'],
                            ))
                    else:
                        locations.append(LocationTuple(
                            id=level_3['id'],
                            name=level_3['name'],
                            country=level_1['country'],
                            level_1=level_1['name'],
                            level_2=level_2['name'],
                            level_3=level_3['name'],
                            level_4=None,
                        ))
        return locations


# TODO: Cache this
def get_locations(domain) -> dict:
    """
    Returns lowest-level locations as id-name pairs
    """
    data_types_by_tag = {  # TODO: Extract this
        dt.tag: dt
        for dt in get_fixture_data_types_in_domain(domain)
    }
    level_4_items = get_fixture_items_for_data_type(
        domain,
        data_types_by_tag["level_4_dcv"]._id
    )
    if len(level_4_items) > 1:
        dicts = (fixture_data_item_to_dict(di) for di in level_4_items)
        return {d['id']: d['name'] for d in dicts}

    level_3_items = get_fixture_items_for_data_type(
        domain,
        data_types_by_tag["level_3_dcv"]._id
    )
    dicts = (fixture_data_item_to_dict(di) for di in level_3_items)
    return {d['id']: d['name'] for d in dicts}


def get_fixture_dicts_by_key(
    domain: str,
    data_type_id: str,
    key: str,
) -> dict:
    dicts_by_key = defaultdict(list)
    for data_item in get_fixture_items_for_data_type(domain, data_type_id):
        dict_ = fixture_data_item_to_dict(data_item)
        dicts_by_key[dict_[key]].append(dict_)
    return dicts_by_key


def fixture_data_item_to_dict(
    data_item: FixtureDataItem,
) -> dict:
    """
    Transforms a FixtureDataItem to a dict.

    A ``FixtureDataItem.fields`` value looks like this::

        {
            'id': FieldList(
                doc_type='FieldList',
                field_list=[
                    FixtureItemField(
                        doc_type='FixtureItemField',
                        field_value='migori_county',
                        properties={}
                    )
                ]
            ),
            'name': FieldList(
                doc_type='FieldList',
                field_list=[
                    FixtureItemField(
                        doc_type='FixtureItemField',
                        field_value='Migori',
                        properties={'lang': 'en'}
                    )
                ]
            ),
            # ... etc. ...
        }

    Only the first value in each ``FieldList`` is selected.

    .. WARNING:: THIS MEANS THAT TRANSLATIONS ARE NOT SUPPORTED.

    The return value for the example above would be::

        {
            'id': 'migori_county',
            'name': 'Migori'
        }

    """
    return {
        key: field_list.field_list[0].field_value
        for key, field_list in data_item.fields.items()
    }
