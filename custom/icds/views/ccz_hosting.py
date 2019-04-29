from __future__ import absolute_import
from __future__ import unicode_literals

from django.http import HttpResponseRedirect
from django.utils.translation import (
    ugettext_lazy,
    ugettext_noop,
    ugettext as _,
)
from django.utils.functional import cached_property
from django.utils.decorators import method_decorator
from django.urls import reverse
from django.shortcuts import redirect
from django.views.generic import TemplateView
from django.contrib import messages
from django.http import HttpResponse

from couchexport.models import Format
from corehq import toggles
from corehq.apps.domain.auth import get_username_and_password_from_request
from corehq.apps.app_manager.dbaccessors import get_brief_apps_in_domain
from corehq.apps.domain.views import (
    BaseDomainView,
    DomainViewMixin,
)
from corehq.apps.hqwebapp.decorators import use_select2_v4
from corehq.apps.locations.permissions import location_safe
from corehq.apps.domain.decorators import login_and_domain_required
from corehq.util.download import get_download_response
from custom.icds.forms import (
    CCZHostingForm,
    CCZHostingLinkForm,
)
from custom.icds.models import (
    CCZHostingLink,
    CCZHosting,
)


@location_safe
@method_decorator([toggles.MANAGE_CCZ_HOSTING.required_decorator()], name='dispatch')
class ManageCCZHostingLink(BaseDomainView):
    urlname = "manage_ccz_hosting_links"
    page_title = ugettext_lazy("Manage CCZ Hosting Links")
    template_name = 'icds/manage_ccz_hosting_links.html'
    section_name = ugettext_lazy("CCZ Hosting Links")

    @cached_property
    def section_url(self):
        return reverse(ManageCCZHostingLink.urlname, args=[self.domain])

    @cached_property
    def form(self):
        return CCZHostingLinkForm(
            data=self.request.POST if self.request.method == "POST" else None
        )

    def get_context_data(self, **kwargs):
        links = [l.to_json() for l in CCZHostingLink.objects.filter(domain=self.domain)]
        return {
            'form': self.form,
            'links': links,
            'domain': self.domain,
        }

    @method_decorator(login_and_domain_required)
    def post(self, request, *args, **kwargs):
        if self.form.is_valid():
            self.form.save()
            return redirect(ManageCCZHostingLink.urlname, domain=self.domain)
        return self.get(request, *args, **kwargs)


class EditCCZHostingLink(ManageCCZHostingLink):
    urlname = "edit_ccz_hosting_link"

    @cached_property
    def form(self):
        link = CCZHostingLink.objects.get(id=self.kwargs['link_id'])
        if self.request.POST:
            return CCZHostingLinkForm(instance=link, data=self.request.POST)
        return CCZHostingLinkForm(instance=link)

    def get_context_data(self, **kwargs):
        return {
            'form': self.form,
            'domain': self.domain,
        }


@location_safe
@method_decorator([toggles.MANAGE_CCZ_HOSTING.required_decorator()], name='dispatch')
class ManageCCZHosting(BaseDomainView):
    urlname = "manage_ccz_hosting"
    page_title = ugettext_lazy("Manage CCZ Hosting")
    template_name = 'icds/manage_ccz_hosting.html'
    section_name = ugettext_noop('CCZ Hostings')

    @use_select2_v4
    def dispatch(self, request, *args, **kwargs):
        return super(ManageCCZHosting, self).dispatch(request, *args, **kwargs)

    @cached_property
    def section_url(self):
        return reverse(ManageCCZHosting.urlname, args=[self.domain])

    @cached_property
    def form(self):
        return CCZHostingForm(
            self.request,
            self.domain,
            data=self.request.POST if self.request.method == "POST" else None
        )

    def get_context_data(self, **kwargs):
        app_names = {app.id: app.name for app in get_brief_apps_in_domain(self.domain, include_remote=True)}
        if self.request.GET.get('link_id'):
            ccz_hostings = CCZHosting.objects.filter(link_id=self.request.GET.get('link_id'))
        else:
            ccz_hostings = CCZHosting.objects.filter(link__domain=self.domain)
        if self.request.GET.get('app_id'):
            ccz_hostings = ccz_hostings.filter(app_id=self.request.GET.get('app_id'))
        version = self.request.GET.get('version')
        if version:
            ccz_hostings = ccz_hostings.filter(version=self.request.GET.get('version'))
        ccz_hostings = [h.to_json(app_names) for h in ccz_hostings]
        return {
            'form': self.form,
            'domain': self.domain,
            'ccz_hostings': ccz_hostings,
            'selected_build_details': ({'id': version, 'text': version} if version else None),
        }

    @method_decorator(login_and_domain_required)
    def post(self, request, *args, **kwargs):
        if self.form.is_valid():
            success, error_message = self.form.save()
            if success:
                return redirect(self.urlname, domain=self.domain)
            else:
                messages.error(request, error_message)
        return self.get(request, *args, **kwargs)


class CCZHostingView(DomainViewMixin, TemplateView):
    urlname = "ccz_hosting"
    page_title = ugettext_lazy("CCZ Hosting")
    template_name = 'icds/ccz_hosting.html'

    @cached_property
    def ccz_hosting_link(self):
        return CCZHostingLink.objects.get(identifier=self.identifier)

    def get(self, request, *args, **kwargs):
        self.identifier = kwargs.get('identifier')
        try:
            ccz_hosting_link = self.ccz_hosting_link
        except CCZHostingLink.DoesNotExist:
            return HttpResponse(status=404)

        uname, passwd = get_username_and_password_from_request(request)
        if uname and passwd:
            if uname == ccz_hosting_link.username and passwd == ccz_hosting_link.get_password:
                return super(CCZHostingView, self).get(request, *args, **kwargs)
        # User did not provide an authorization header or gave incorrect credentials.
        response = HttpResponse(status=401)
        response['WWW-Authenticate'] = 'Basic realm="%s"' % ''
        return response

    def get_context_data(self, **kwargs):
        app_names = {app.id: app.name for app in get_brief_apps_in_domain(self.domain, include_remote=True)}
        return {
            'page_title': _("%s CommCare Files" % self.identifier.capitalize()),
            'ccz_hostings': [h.to_json(app_names) for h in CCZHosting.objects.filter(link=self.ccz_hosting_link)]
        }


@login_and_domain_required
def remove_ccz_hosting(request, domain, hosting_id):
    try:
        ccz_hosting = CCZHosting.objects.get(pk=hosting_id)
        ccz_hosting.delete()
    except CCZHosting.DoesNotExist:
        pass
    return HttpResponseRedirect(reverse(ManageCCZHosting.urlname, args=[domain]))


@location_safe
def download_ccz(request, domain, hosting_id, blob_id):
    ccz_hosting = CCZHosting.objects.get(pk=hosting_id)
    assert ccz_hosting.blob_id == blob_id
    file_size = ccz_hosting.utility.ccz_file_meta.content_length
    file_name = ccz_hosting.utility.ccz_file_meta.name
    content_format = Format('', Format.ZIP, '', True)
    return get_download_response(ccz_hosting.utility.ccz_file, file_size, content_format, file_name,
                                 request)
