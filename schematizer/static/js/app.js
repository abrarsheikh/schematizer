(function() {
    var docToolApp = angular.module('docToolApp', [
        'ngRoute',
        'navbar',
        'home',
        'tableView'
    ]);

    docToolApp.constant('CONSTANTS', {
        allCategories: '[ All Categories ]',
        uncategorized: '[ Uncategorized ]',
        defaultSchema: 'yelp_dw_redshift.public'
    });

    docToolApp.service('DocToolService', function() {
        return {
            formatSchema: function(schema) {
                if (schema !== undefined) {
                    // If the string is in the format aaa.bbb, return aaa.
                    return schema;
                }
            }
        };
    });

    "use strict";
    docToolApp.config(['$routeProvider', 'CONSTANTS',
        function($routeProvider, CONSTANTS) {
            $routeProvider.
            when('/home', {
                templateUrl: 'partials/home.html',
                controller: 'HomeController'
            }).
            when('/table', {
                templateUrl: 'partials/table-view.html',
                controller: 'TableViewController'
            }).
            when('/about', {
                templateUrl: 'partials/about.html',
                controller: 'AboutController'
            }).
            otherwise({
                redirectTo: '/home'
            });
        }
    ]);
})();
