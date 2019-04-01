/**
 * todo add docstring
 */

hqDefine('reports/v2/js/datagrid/columns', [
    'jquery',
    'knockout',
    'underscore',
    'reports/v2/js/datagrid/filters',
], function (
    $,
    ko,
    _,
    filters
) {
    'use strict';

    var columnModel = function (data, availableFilters) {
        var self = {};

        self.title = ko.observable(data.title);
        self.slug = ko.observable(data.slug);
        self.width = ko.observable(data.width || 200);

        self.appliedFilters = ko.observableArray(_.map(data.appliedFilters, function (filterData) {
            var filterModel = filters.appliedColumnFilterModel(filterData);
            if (availableFilters) {
                filterModel.filter(ko.utils.arrayFirst(availableFilters(), function (item) {
                    return item.name() === filterModel.filter().name() && item.filterType() === filterModel.filter().filterType();
                }));
            }
            return filterModel;
        }));

        self.unwrap = function () {
            return ko.mapping.toJS(self);
        };

        return self;
    };

    var editColumnController = function (options) {
        var self = {};

        self.slugEndpoint = options.slugEndpoint;
        self.reportContext = options.reportContext;

        self.availableFilters = ko.observableArray(_.map(options.availableFilters, function (data) {
            return filters.columnFilterModel(data);
        }));

        self.slugOptions = ko.observableArray();

        self.oldColumn = ko.observable();
        self.column = ko.observable();
        self.isNew = ko.observable();

        self.setNew = function () {
            self.reloadOptions();
            self.oldColumn(undefined);

            if (self.isNew() && self.column()) {
                // keep state of existing add column progress
                self.column(columnModel(self.column().unwrap(), self.availableFilters));
            } else {
                self.column(columnModel({}));
                self.isNew(true);
            }
        };

        self.set = function (existingColumn) {
            self.reloadOptions();
            self.oldColumn(columnModel(existingColumn).unwrap());
            self.column(columnModel(existingColumn.unwrap(), self.availableFilters));
            self.isNew(false);
        };

        self.unset = function () {
            self.oldColumn(undefined);
            self.column(undefined);
            self.isNew(false);
        };

        self.reloadOptions = function () {
            // reload slug options
            $.ajax({
                url: self.slugEndpoint.getUrl(),
                method: 'post',
                dataType: 'json',
                data: {
                    reportContext: JSON.stringify(self.reportContext()),
                },
            })
                .done(function (data) {
                    self.slugOptions(data.options);
                });
        };

        self.isColumnValid = ko.computed(function () {
            if (self.column()) {
                return !!self.column().title() && !!self.column().slug();
            }
            return false;
        });

        self.isSaveDisabled = ko.computed(function () {
            return !self.isColumnValid();
        });

        return self;
    };

    return {
        columnModel: columnModel,
        editColumnController: editColumnController,
    };
});
