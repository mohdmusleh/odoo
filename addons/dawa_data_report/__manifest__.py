{
    "name": "Dawa Data Report",
    "summary": "Connect to server and generate report",
    "description": """
        Connect to server and generate report like
        SO-POS reports
        PO reports
    """,
    "version": "17.0.0.0.2",
    "author": "Dawatech",
    "website": "https://dawatech.com",
    "category": "Inventory/Inventory",
    "depends": ["base", "purchase", "sale", "stock"],
    "data": [
        "security/ir.model.access.csv",
        "wizard/dawa_data_report_wizard_views.xml",
    ],
    "license": "AGPL-3",
    "installable": True,
    "application": False,
}
