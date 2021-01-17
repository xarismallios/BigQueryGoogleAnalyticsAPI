# API functionalities

The api provide 4 different endpoints to use:

- /report/cvr/<int:days> : provides conversion rate for last x days
- /report/cvr/compare-periods/<int:days> : provides conversion rate for last x days and the previous period
- /report/cvr/compare-periods/device/usertype/<int:days> : provides conversion rate for last x days and the previous period groupped by device and user type
- /report/cvr/<int:days>/csv : exports conversion rate for last x days in a csv file
- /report/userprofile/<int:id> : exports user's profile schema

To succesfully run the api you must include your credentials from google cloud serivce account on your folder path with the name "key.json".


# You can see the app deployed live here : https://malliosapi.herokuapp.com/

To get a csv export for conversion rate last 7 days click here : https://malliosapi.herokuapp.com/report/cvr/7/csv
