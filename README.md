# xarisreport

The api provide 4 different endpoints to use:

- /report/cvr/<int:days> : provides conversion rate for last x days
- /report/cvr/compare-periods/<int:days> : provides conversion rate for last x days and the previous period
- /report/cvr/compare-periods/device/usertype/<int:days> : provides conversion rate for last x days and the previous period groupped by device and user type
- /report/cvr/<int:days>/csv : exports conversion rate for last x days in a csv file
- /report/userprofile/<int:id> : exports user's profile schema
