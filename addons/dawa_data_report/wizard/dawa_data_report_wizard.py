import base64
import logging
import os
import sys
import time
from datetime import datetime
from io import BytesIO
from threading import Thread

import pandas as pd
import paramiko
import xlsxwriter
from dateutil.relativedelta import relativedelta

from odoo import _, api, fields, models
from odoo.exceptions import UserError, ValidationError

logger = logging.getLogger(__name__)

loading_animation_active = True


def loading_animation():
    while loading_animation_active:
        sys.stdout.write("\rLoading    ")
        sys.stdout.flush()
        time.sleep(0.5)
        if not loading_animation_active:
            break
        sys.stdout.write("\rLoading .  ")
        sys.stdout.flush()
        time.sleep(0.5)
        if not loading_animation_active:
            break
        sys.stdout.write("\rLoading .. ")
        sys.stdout.flush()
        time.sleep(0.5)
        if not loading_animation_active:
            break
        sys.stdout.write("\rLoading ...")
        sys.stdout.flush()
        time.sleep(0.5)
    sys.stdout.flush()


class DataReport(models.TransientModel):
    _name = "data.report"
    _description = "Dawatech Data Report"

    report_type = fields.Selection(
        [
            ("por", "Purchase Order report"),
            ("so_pos_r", "Sale Order / Point of Sale report"),
            (
                "total_trans_pm_r",
                "Total Number of transactions per Month for Purchases, Sales and POS",
            ),
            ("total_po_pm_r", "Total “Purchases” per month"),
            ("so_pos_all_r", "Sale Order / Point of Sale report ( ALL )"),
            ("total_users", "Number of Users on each server."),
            ("total_pos", "Number of Point of Sales on each server."),
        ],
        string="Report Type",
    )
    user_name = fields.Char(string="User")
    txt_file = fields.Binary()
    product_list = fields.Char(
        string="Product",
        help="You can use barcode number or product name, divided by ','",
    )
    ssh_key_path = fields.Char(string="SSH Key Path")
    hide_product = fields.Boolean(
        compute="_compute_hide_product", string="Hide Product"
    )
    file_name = fields.Char(string="File Name")
    start_date = fields.Date(string="Start Date")
    end_date = fields.Date(string="End Date")
    hide_date = fields.Boolean(compute="_compute_hide_date")

    @api.depends("report_type")
    def _compute_hide_product(self):
        if self.report_type in [
            "total_trans_pm_r",
            "total_po_pm_r",
            "so_pos_all_r",
            "total_users",
            "total_pos",
        ]:
            self.hide_product = True
        else:
            self.hide_product = False

    @api.depends("report_type")
    def _compute_hide_date(self):
        if self.report_type in [
            "total_trans_pm_r",
            "total_po_pm_r",
            "total_users",
            "total_pos",
        ]:
            self.hide_date = True
        else:
            self.hide_date = False

    def get_report(self):
        if not all(
            [self.report_type, self.user_name, self.ssh_key_path, self.txt_file]
        ):
            raise ValidationError(_("Please fill in all required fields."))

        if self.file_name and os.path.splitext(self.file_name)[-1] != ".txt":
            raise ValidationError(_("Selected file is not a TXT file."))

        report_functions = {
            "por": self.process_po_report,
            "so_pos_r": self.process_so_pos_report,
            "total_trans_pm_r": self.process_total_no_of_tran_report,
            "total_po_pm_r": self.process_total_purchase_per_month,
            "so_pos_all_r": self.process_so_pos_report_total,
            "total_users": self.process_total_users,
            "total_pos": self.process_total_pos,
        }

        processing_function = report_functions.get(self.report_type)
        if processing_function:
            if self.report_type in [
                "total_trans_pm_r",
                "total_po_pm_r",
                "so_pos_all_r",
                "total_users",
                "total_pos",
            ]:
                file = processing_function(self.user_name, self.ssh_key_path)
            else:
                file = processing_function(
                    self.user_name, self.ssh_key_path, self.product_list
                )

            return {
                "type": "ir.actions.act_url",
                "url": file,
                "target": "new",
            }

    def process_po_report(self, username, private_key_path, product_list):
        if not product_list:
            raise UserError(_("Please add products"))
        product_list = product_list.split(",")
        products = tuple(product_list)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        temp_txt_file = base64.b64decode(self.txt_file).decode()
        servers = temp_txt_file
        outputs = []
        for server in servers.split(","):
            thread = Thread(target=loading_animation)
            server = server.strip()
            logger.info("Connecting to %s...", server)
            ssh.connect(
                hostname=server, username=username, key_filename=private_key_path
            )
            thread = Thread(target=loading_animation)
            thread.start()
            table = "purchase_order"
            po_report = f"""
                SELECT po.date_order,
                rp.name,
                pt.name,
                pp.barcode,
                pol.product_qty,
                pol.public_price,
                pol.price_subtotal,
                po.name,
                pol.bonus_qty
                FROM {table} as po
                LEFT JOIN purchase_order_line as pol on po.id = pol.order_id
                LEFT JOIN product_product as pp on pol.product_id = pp.id
                LEFT JOIN product_template as pt on pp.product_tmpl_id = pt.id
                LEFT JOIN res_partner as rp on pt.agent = rp.id
                WHERE """
            if self.start_date and self.end_date:
                sd = self.start_date
                ed = self.end_date
                po_report += f" po.date_order BETWEEN '{sd}' AND '{ed}' AND"

            if len(product_list) == 1:
                pro_name = product_list[0]
                po_report += f" pp.barcode = '{pro_name}' OR pt.name='{pro_name}'"
            elif len(product_list) > 1:
                po_report += f" pp.barcode in {products} OR pt.name in {products}"

            stdin, stdout, stderr = ssh.exec_command(
                f'su postgres -c "psql -d {server} -t -A -F"," -c \\"{po_report}\\""'
            )
            logger.info("stdin data %s...", stdin)
            logger.info("stdin data %s...", stderr)
            output = [
                tuple(data.split(",") + [server])
                for data in stdout.read().decode().split("\n")
                if data
            ]
            outputs.append(output)
            ssh.close()
        file_attachment = self.create_xls_file(outputs)
        return file_attachment

    def create_xls_file(self, outputs):
        filename = "report_1_purchase_order.xlsx"
        file_data = BytesIO()
        workbook = xlsxwriter.Workbook(file_data)
        worksheet = workbook.add_worksheet()
        col = 0
        row = 0
        worksheet.write(row, col, "Date")
        worksheet.write(row, col + 1, "Agent")
        worksheet.write(row, col + 2, "Product ID")
        worksheet.write(row, col + 3, "Barcode")
        worksheet.write(row, col + 4, "Product Quantity")
        worksheet.write(row, col + 5, "Sale Price")
        worksheet.write(row, col + 6, "Calculated Cost")
        worksheet.write(row, col + 7, "Order Name")
        worksheet.write(row, col + 8, "Bonus Quantity")
        worksheet.write(row, col + 9, "Server")
        row += 1
        for output in outputs:
            for t_row in output:
                worksheet.write(row, col, "" if str(t_row[0]) == "nan" else t_row[0])
                worksheet.write(
                    row,
                    col + 1,
                    "" if str(t_row[1]) == "nan" else t_row[1],
                )
                worksheet.write(
                    row,
                    col + 2,
                    "" if str(t_row[2]) == "nan" else t_row[2],
                )
                worksheet.write(
                    row,
                    col + 3,
                    "" if str(t_row[3]) == "nan" else t_row[3],
                )
                worksheet.write(
                    row,
                    col + 4,
                    "" if str(t_row[4]) == "nan" else t_row[4],
                )
                worksheet.write(
                    row,
                    col + 5,
                    "" if str(t_row[5]) == "nan" else t_row[5],
                )
                worksheet.write(
                    row,
                    col + 6,
                    "" if str(t_row[6]) == "nan" else t_row[6],
                )
                worksheet.write(
                    row,
                    col + 7,
                    "" if str(t_row[7]) == "nan" else t_row[7],
                )
                worksheet.write(
                    row,
                    col + 8,
                    "" if str(t_row[8]) == "nan" else t_row[8],
                )
                worksheet.write(
                    row, col + 9, "" if str(t_row[9]) == "nan" else t_row[9]
                )
                row += 1

        file_data.seek(0)
        workbook.close()
        result = base64.encodebytes(file_data.getvalue())

        excel_file = self.env["ir.attachment"].create(
            {
                "name": filename,
                "datas": result,
                "res_model": "data.report",
                "type": "binary",
            }
        )
        file_path = f"/web/content/{excel_file.id}?download=true"
        return file_path

    def process_so_pos_report(self, username, private_key_path, product_list):
        if not product_list:
            raise UserError(_("Please add products"))
        product_list = product_list.split(",")
        products = tuple(product_list)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        temp_txt_file = base64.b64decode(self.txt_file).decode()
        servers = temp_txt_file
        outputs = []
        for server in servers.split(","):
            thread = Thread(target=loading_animation)
            server = server.strip()
            logger.info("Connecting to %s...", server)
            ssh.connect(
                hostname=server, username=username, key_filename=private_key_path
            )

            thread = Thread(target=loading_animation)
            thread.start()

            sol_table = "sale_order_line"
            so_report = f"""SELECT DISTINCT ON (so.id)
                       so.date_order,
                       rp.name,
                       pt.name,
                       pp.barcode,
                       sol.product_uom_qty,
                       sol.price_unit,
                       sol.price_subtotal,
                       so.name
                    FROM {sol_table} as sol
                         LEFT JOIN sale_order as so on so.id = sol.order_id
                         LEFT JOIN product_product as pp on sol.product_id = pp.id
                         LEFT JOIN product_template as pt on pp.product_tmpl_id = pt.id
                         LEFT JOIN res_partner as rp on pt.agent = rp.id
                    WHERE sol.product_uom_qty > 0"""
            if self.start_date and self.end_date:
                sd = self.start_date
                ed = self.end_date
                so_report += f" AND so.date_order BETWEEN '{sd}' AND '{ed}'"
            if len(product_list) == 1:
                pro_name = product_list[0]
                so_report += f" AND pp.barcode = '{pro_name}' OR pt.name='{pro_name}'"
            elif len(product_list) > 1:
                so_report += f" AND pp.barcode in {products} OR pt.name in {products}"
            so_report += " ORDER BY so.id, so.date_order"
            stdin, stdout, stderr = ssh.exec_command(
                f'su postgres -c "psql -d {server} -t -A -F"," -c \\"{so_report}\\""'
            )
            logger.info("stdin data %s...", stdin)
            logger.info("stdin data %s...", stderr)
            output = [
                tuple(data.split(",") + [server])
                for data in stdout.read().decode().split("\n")
                if data
            ]
            outputs.append(output)

            pol_table = "pos_order_line"
            pos_report = f"""SELECT pol.date_order,
                       rp.name,
                       pt.name,
                       pp.barcode,
                       pol.qty,
                       pol.price_unit,
                       pol.price_subtotal_incl,
                       pol.name
                FROM {pol_table} as pol
                         LEFT JOIN sale_order as so on so.id = pol.order_id
                         LEFT JOIN product_product as pp on pol.product_id = pp.id
                         LEFT JOIN product_template as pt on pp.product_tmpl_id = pt.id
                         LEFT JOIN res_partner as rp on pt.agent = rp.id
                WHERE pol.qty > 0"""
            if self.start_date and self.end_date:
                sd = self.start_date
                ed = self.end_date
                pos_report += f" AND pol.date_order BETWEEN '{sd}' AND '{ed}'"

            if len(product_list) == 1:
                pro_name = product_list[0]
                pos_report += f" AND pp.barcode = '{pro_name}' OR pt.name='{pro_name}'"
            elif len(product_list) > 1:
                pos_report += f" AND pp.barcode in {products} OR pt.name in {products}"

            stdin, stdout, stderr = ssh.exec_command(
                f'su postgres -c "psql -d {server} -t -A -F"," -c \\"{pos_report}\\""'
            )
            logger.info("stdin data %s...", stdin)
            logger.info("stdin data %s...", stderr)
            output = [
                tuple(data.split(",") + [server])
                for data in stdout.read().decode().split("\n")
                if data
            ]
            outputs[0] += output

            ssh.close()
        file_attachment = self.create_xls_so_pos_report(outputs)
        return file_attachment

    def create_xls_so_pos_report(self, result_df):
        filename = "report_2_so_pos_order.xlsx"
        file_data = BytesIO()
        workbook = xlsxwriter.Workbook(file_data)
        worksheet = workbook.add_worksheet()
        col = 0
        row = 0
        worksheet.write(row, col, "Date")
        worksheet.write(row, col + 1, "Agent")
        worksheet.write(row, col + 2, "Product Name")
        worksheet.write(row, col + 3, "Barcode")
        worksheet.write(row, col + 4, "Product Quantity")
        worksheet.write(row, col + 5, "Public Price")
        worksheet.write(row, col + 6, "Total")
        worksheet.write(row, col + 7, "Order Name")
        worksheet.write(row, col + 8, "Server")
        row += 1
        for t_row in result_df[0]:
            worksheet.write(row, col, "" if str(t_row[0]) == "nan" else t_row[0])
            worksheet.write(row, col + 1, "" if str(t_row[1]) == "nan" else t_row[1])
            worksheet.write(row, col + 2, "" if str(t_row[2]) == "nan" else t_row[2])
            worksheet.write(row, col + 3, "" if str(t_row[3]) == "nan" else t_row[3])
            worksheet.write(row, col + 4, "" if str(t_row[4]) == "nan" else t_row[4])
            worksheet.write(row, col + 5, "" if str(t_row[5]) == "nan" else t_row[5])
            worksheet.write(row, col + 6, "" if str(t_row[6]) == "nan" else t_row[6])
            worksheet.write(row, col + 7, "" if str(t_row[7]) == "nan" else t_row[7])
            worksheet.write(row, col + 8, "" if str(t_row[8]) == "nan" else t_row[8])
            row += 1

        file_data.seek(0)
        workbook.close()
        result = base64.encodebytes(file_data.getvalue())

        excel_file = self.env["ir.attachment"].create(
            {
                "name": filename,
                "datas": result,
                "res_model": "data.report",
                "type": "binary",
            }
        )
        file_path = f"/web/content/{excel_file.id}?download=true"
        return file_path

    def process_total_no_of_tran_report(self, user_name, ssh_key_path):
        reports = ["purchase_order", "sale_order", "pos_order"]
        private_key_path = ssh_key_path
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        temp_txt_file = base64.b64decode(self.txt_file).decode()
        servers = temp_txt_file

        result_df = pd.DataFrame(
            columns=[
                "Year",
                "Month",
                "Total # Transactions PO",
                "Total # Transactions SO",
                "Total # Transactions POS",
                "Server",
            ]
        )

        for server in servers.split(","):
            server = server.strip()
            logger.info("Connecting to %s...", server)
            ssh.connect(
                hostname=server, username=user_name, key_filename=private_key_path
            )

            thread = Thread(target=loading_animation)
            thread.start()

            for report in reports:
                query = f"SELECT MIN(date_order) FROM {report}"

                stdin, stdout, stderr = ssh.exec_command(
                    f'su postgres -c "psql -d {server} -t -A -F"," -c \\"{query}\\""'
                )
                logger.info("stdin data %s...", stdin)
                logger.info("stdin data %s...", stderr)
                earliest_date_str = stdout.read().decode().strip()

                try:
                    earliest_date = datetime.strptime(
                        earliest_date_str, "%Y-%m-%d %H:%M:%S.%f"
                    )
                except ValueError:
                    earliest_date = (
                        datetime.strptime(earliest_date_str, "%Y-%m-%d %H:%M:%S")
                        if earliest_date_str
                        else None
                    )

                if earliest_date is None:
                    logger.warning("No data available in the database on %s.", server)
                    continue

                current_date = earliest_date.replace(day=1)

                while current_date <= datetime.now():
                    result_df = self.process_month_data(
                        ssh, server, current_date, result_df, table_name=report
                    )
                    current_date = (
                        current_date.replace(year=current_date.year + 1, month=1)
                        if current_date.month == 12
                        else current_date.replace(month=current_date.month + 1)
                    )
                logger.warning("Data for %s collected", report)
            logger.warning("All data for %s collected", server)
            ssh.close()
        file_attachment = self.create_total_no_of_tran_report(result_df)
        return file_attachment

    def create_total_no_of_tran_report(self, result_df):
        filename = "report_3_total_number_of_trans_pm_for_po.xlsx"
        file_data = BytesIO()
        workbook = xlsxwriter.Workbook(file_data)
        worksheet = workbook.add_worksheet()
        col = 0
        row = 0
        worksheet.set_column(2, 5, 20)
        worksheet.write(row, col, "Year")
        worksheet.write(row, col + 1, "Month")
        worksheet.write(row, col + 2, "Total # Transactions PO")
        worksheet.write(row, col + 3, "Total # Transactions SO")
        worksheet.write(row, col + 4, "Total # Transactions POS")
        worksheet.write(row, col + 5, "Server")
        row += 1
        for _index, t_row in result_df.iterrows():
            worksheet.write(
                row, col, "" if str(t_row.tolist()[0]) == "nan" else t_row.tolist()[0]
            )
            worksheet.write(
                row,
                col + 1,
                "" if str(t_row.tolist()[1]) == "nan" else t_row.tolist()[1],
            )
            worksheet.write(
                row,
                col + 2,
                "" if str(t_row.tolist()[2]) == "nan" else t_row.tolist()[2],
            )
            worksheet.write(
                row,
                col + 3,
                "" if str(t_row.tolist()[3]) == "nan" else t_row.tolist()[3],
            )
            worksheet.write(
                row,
                col + 4,
                "" if str(t_row.tolist()[4]) == "nan" else t_row.tolist()[4],
            )
            worksheet.write(
                row,
                col + 5,
                "" if str(t_row.tolist()[5]) == "nan" else t_row.tolist()[5],
            )
            row += 1

        file_data.seek(0)
        workbook.close()
        result = base64.encodebytes(file_data.getvalue())

        excel_file = self.env["ir.attachment"].create(
            {
                "name": filename,
                "datas": result,
                "res_model": "data.report",
                "type": "binary",
            }
        )
        file_path = f"/web/content/{excel_file.id}?download=true"
        return file_path

    def process_total_purchase_per_month(self, username, private_key_path):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        temp_txt_file = base64.b64decode(self.txt_file).decode()
        servers = temp_txt_file

        result_df = pd.DataFrame(columns=["Year", "Month", "Total Amount", "Server"])
        for server in servers.split(","):
            thread = Thread(target=loading_animation)
            server = server.strip()
            logger.info("Connecting to %s...", server)
            ssh.connect(
                hostname=server, username=username, key_filename=private_key_path
            )
            thread.start()

            date_query = "SELECT MIN(date_order) FROM purchase_order"

            stdin, stdout, stderr = ssh.exec_command(
                f'su postgres -c "psql -d {server} -t -A -F"," -c \\"{date_query}\\""'
            )
            logger.info("stdin data %s...", stdin)
            logger.info("stdin data %s...", stderr)
            earliest_date_str = stdout.read().decode().strip()

            try:
                earliest_date = datetime.strptime(
                    earliest_date_str, "%Y-%m-%d %H:%M:%S.%f"
                )
            except ValueError:
                earliest_date = (
                    datetime.strptime(earliest_date_str, "%Y-%m-%d %H:%M:%S")
                    if earliest_date_str
                    else None
                )

            if earliest_date is None:
                logger.warning("No data available in the database on %s.", server)
                ssh.close()
                continue

            current_date = earliest_date.replace(day=1)
            while current_date <= datetime.now():
                result_df = self.process_month_amount_data(
                    ssh, server, current_date, result_df
                )
                current_date = current_date + relativedelta(months=1)

            logger.warning("All data for %s collected", server)
            ssh.close()
        file_path = self.create_total_purchase_per_month_report(result_df)
        return file_path

    def create_total_purchase_per_month_report(self, result_df):
        filename = "report_4_total_amount.xlsx"
        file_data = BytesIO()
        workbook = xlsxwriter.Workbook(file_data)
        worksheet = workbook.add_worksheet()
        col = 0
        row = 0
        worksheet.set_column(0, 1, 10)
        worksheet.set_column(2, 2, 15)
        worksheet.set_column(3, 3, 20)
        worksheet.write(row, col, "Year")
        worksheet.write(row, col + 1, "Month")
        worksheet.write(row, col + 2, "Total Amount")
        worksheet.write(row, col + 3, "Server")
        row += 1
        for _index, t_row in result_df.iterrows():
            worksheet.write(row, col, t_row.tolist()[0])
            worksheet.write(row, col + 1, t_row.tolist()[1])
            worksheet.write(row, col + 2, t_row.tolist()[2])
            worksheet.write(row, col + 3, t_row.tolist()[3])
            row += 1

        file_data.seek(0)
        workbook.close()
        result = base64.encodebytes(file_data.getvalue())

        excel_file = self.env["ir.attachment"].create(
            {
                "name": filename,
                "datas": result,
                "res_model": "data.report",
                "type": "binary",
            }
        )
        file_path = f"/web/content/{excel_file.id}?download=true"
        return file_path

    def process_so_pos_report_total(self, username, private_key_path):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        temp_txt_file = base64.b64decode(self.txt_file).decode()
        servers = temp_txt_file

        for server in servers.split(","):
            thread = Thread(target=loading_animation)
            server = server.strip()
            logger.info("Connection to %s. Processing report", server)
            thread.start()
            ssh.connect(
                hostname=server, username=username, key_filename=private_key_path
            )
            sol_table = "sale_order_line"
            so_sql_query = f"""SELECT DISTINCT ON (so.id)
                       so.date_order,
                       rp.name,
                       pt.name,
                       pp.barcode,
                       sol.product_uom_qty,
                       sol.price_unit,
                       sol.price_subtotal,
                       so.name
                FROM {sol_table} as sol
                         LEFT JOIN sale_order as so on so.id = sol.order_id
                         LEFT JOIN product_product as pp on sol.product_id = pp.id
                         LEFT JOIN product_template as pt on pp.product_tmpl_id = pt.id
                         LEFT JOIN res_partner as rp on pt.agent = rp.id
                WHERE sol.product_uom_qty > 0
                """
            if self.start_date and self.end_date:
                sd = self.start_date
                ed = self.end_date
                so_sql_query += f" AND so.date_order BETWEEN '{sd}' AND '{ed}'"
            so_sql_query += " ORDER BY so.id, so.date_order"
            stdin, stdout, stderr = ssh.exec_command(
                f'su postgres -c "psql -d {server} -t -A -F"," -c \\"{so_sql_query}\\""'
            )
            logger.info("stdin data %s...", stdin)
            logger.info("stdin data %s...", stderr)
            output = [
                tuple(data.split(",") + [server])
                for data in stdout.read().decode().split("\n")
                if data
            ]
            outputs = []
            outputs.append(output)
            pol_table = "pos_order_line"
            to_pos_query = f"""SELECT pol.date_order,
                       rp.name,
                       pt.name,
                       pp.barcode,
                       pol.qty,
                       pol.price_unit,
                       pol.price_subtotal_incl,
                       pol.name
                FROM {pol_table} as pol
                         LEFT JOIN sale_order as so on so.id = pol.order_id
                         LEFT JOIN product_product as pp on pol.product_id = pp.id
                         LEFT JOIN product_template as pt on pp.product_tmpl_id = pt.id
                         LEFT JOIN res_partner as rp on pt.agent = rp.id
                WHERE pol.qty > 0"""
            if self.start_date and self.end_date:
                sd = self.start_date
                ed = self.end_date
                to_pos_query += f" AND pol.date_order BETWEEN '{sd}' AND '{ed}'"
            stdin, stdout, stderr = ssh.exec_command(
                f'su postgres -c "psql -d {server} -t -A -F"," -c \\"{to_pos_query}\\""'
            )
            logger.info("stdin data %s...", stdin)
            logger.info("stdin data %s...", stderr)
            output = [
                tuple(data.split(",") + [server])
                for data in stdout.read().decode().split("\n")
                if data
            ]
            outputs[0] += output
            ssh.close()
            file_path = self.generate_so_pos_total_excel_report(outputs)
            return file_path

    def generate_so_pos_total_excel_report(self, result_df):
        filename = "report_5_so_pos_order_total.xlsx"
        file_data = BytesIO()
        workbook = xlsxwriter.Workbook(file_data)
        worksheet = workbook.add_worksheet()
        col = 0
        row = 0
        worksheet.set_column(0, 9, 20)
        worksheet.set_column(2, 2, 30)
        worksheet.write(row, col, "Date")
        worksheet.write(row, col + 1, "Agent")
        worksheet.write(row, col + 2, "Product Name")
        worksheet.write(row, col + 3, "Barcode")
        worksheet.write(row, col + 4, "Product Quantity")
        worksheet.write(row, col + 5, "Public Price")
        worksheet.write(row, col + 6, "Total")
        worksheet.write(row, col + 7, "Order Name")
        worksheet.write(row, col + 8, "Server")
        row += 1
        for t_row in result_df[0]:
            worksheet.write(row, col, "" if str(t_row[0]) == "nan" else t_row[0])
            worksheet.write(row, col + 1, "" if str(t_row[1]) == "nan" else t_row[1])
            worksheet.write(row, col + 2, "" if str(t_row[2]) == "nan" else t_row[2])
            worksheet.write(row, col + 3, "" if str(t_row[3]) == "nan" else t_row[3])
            worksheet.write(row, col + 4, "" if str(t_row[4]) == "nan" else t_row[4])
            worksheet.write(row, col + 5, "" if str(t_row[5]) == "nan" else t_row[5])
            worksheet.write(row, col + 6, "" if str(t_row[6]) == "nan" else t_row[6])
            worksheet.write(row, col + 7, "" if str(t_row[7]) == "nan" else t_row[7])
            worksheet.write(row, col + 8, "" if str(t_row[8]) == "nan" else t_row[8])
            row += 1
        file_data.seek(0)
        workbook.close()
        result = base64.encodebytes(file_data.getvalue())

        excel_file = self.env["ir.attachment"].create(
            {
                "name": filename,
                "datas": result,
                "res_model": "data.report",
                "type": "binary",
            }
        )
        file_path = f"/web/content/{excel_file.id}?download=true"
        return file_path

    def process_total_pos(self, username, private_key_path):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        temp_txt_file = base64.b64decode(self.txt_file).decode()
        servers = temp_txt_file

        result_df = pd.DataFrame(columns=["Name", "Server"])
        for server in servers.split(","):
            thread = Thread(target=loading_animation)
            server = server.strip()
            logger.info("Connecting to %s...", server)
            ssh.connect(
                hostname=server, username=username, key_filename=private_key_path
            )
            thread.start()

            pos_query = "SELECT name FROM pos_config"

            stdin, stdout, stderr = ssh.exec_command(
                f'su postgres -c "psql -d {server} -t -A -F"," -c \\"{pos_query}\\""'
            )
            logger.info("stdin data %s...", stdin)
            logger.info("stdin data %s...", stderr)
            earliest_date_str = stdout.read().decode().strip()

            for earliest_str in earliest_date_str.split("\n"):
                new_dic = {
                    "Name": earliest_str,
                    "Server": server,
                }
                result_df = pd.concat(
                    [result_df, pd.DataFrame([new_dic])], ignore_index=True
                )
            logger.warning("All data for %s collected", server)
            ssh.close()
        file_path = self.create_total_pos_report(result_df)
        return file_path

    def create_total_pos_report(self, result_df):
        filename = "report_6_total_pos.xlsx"
        file_data = BytesIO()
        workbook = xlsxwriter.Workbook(file_data)
        worksheet = workbook.add_worksheet()
        col = 0
        row = 0
        worksheet.set_column(0, 3, 20)
        worksheet.write(row, col, "Name")
        worksheet.write(row, col + 1, "Server")
        row += 1
        for _index, t_row in result_df.iterrows():
            worksheet.write(
                row, col, "" if str(t_row.tolist()[0]) == "nan" else t_row.tolist()[0]
            )
            worksheet.write(
                row,
                col + 1,
                "" if str(t_row.tolist()[1]) == "nan" else t_row.tolist()[1],
            )
            row += 1

        file_data.seek(0)
        workbook.close()
        result = base64.encodebytes(file_data.getvalue())

        excel_file = self.env["ir.attachment"].create(
            {
                "name": filename,
                "datas": result,
                "res_model": "data.report",
                "type": "binary",
            }
        )
        file_path = f"/web/content/{excel_file.id}?download=true"
        return file_path

    def process_total_users(self, username, private_key_path):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        temp_txt_file = base64.b64decode(self.txt_file).decode()
        servers = temp_txt_file

        result_df = pd.DataFrame(columns=["Name", "Email", "Server"])
        for server in servers.split(","):
            thread = Thread(target=loading_animation)
            server = server.strip()
            logger.info("Connecting to %s...", server)
            ssh.connect(
                hostname=server, username=username, key_filename=private_key_path
            )
            thread.start()

            table = "res_users"
            user_query = f"""
                SELECT
                rp.name,
                ru.login
                FROM {table} as ru
                LEFT JOIN res_partner as rp on ru.partner_id = rp.id;
                """

            stdin, stdout, stderr = ssh.exec_command(
                f'su postgres -c "psql -d {server} -t -A -F"," -c \\"{user_query}\\""'
            )
            logger.info("stdin data %s...", stdin)
            logger.info("stdin data %s...", stderr)
            earliest_date_str = stdout.read().decode().strip()

            for earliest in earliest_date_str.split("\n"):
                earliest_str = earliest.split(",")
                new_dic = {
                    "Name": earliest_str[0],
                    "Email": earliest_str[1],
                    "Server": server,
                }
                result_df = pd.concat(
                    [result_df, pd.DataFrame([new_dic])], ignore_index=True
                )
            logger.warning("All data for %s collected", server)
            ssh.close()
        file_path = self.create_total_users_report(result_df)
        return file_path

    def create_total_users_report(self, result_df):
        filename = "report_7_total_users.xlsx"
        file_data = BytesIO()
        workbook = xlsxwriter.Workbook(file_data)
        worksheet = workbook.add_worksheet()
        col = 0
        row = 0
        for i in range(0, 3):
            worksheet.set_column(i, i, 25)
        worksheet.write(row, col, "Name")
        worksheet.write(row, col + 1, "Email")
        worksheet.write(row, col + 2, "Server")
        row += 1
        for _index, t_row in result_df.iterrows():
            worksheet.write(row, col, t_row.tolist()[0])
            worksheet.write(row, col + 1, t_row.tolist()[1])
            worksheet.write(row, col + 2, t_row.tolist()[2])
            row += 1

        file_data.seek(0)
        workbook.close()
        result = base64.encodebytes(file_data.getvalue())

        excel_file = self.env["ir.attachment"].create(
            {
                "name": filename,
                "datas": result,
                "res_model": "data.report",
                "type": "binary",
            }
        )
        file_path = f"/web/content/{excel_file.id}?download=true"
        return file_path

    def process_month_amount_data(
        self,
        ssh: paramiko.server,
        server: str,
        current_date: datetime,
        result_df: pd.DataFrame,
    ):
        sql_query = (
            "SELECT SUM(amount_total) "
            "FROM purchase_order "
            "WHERE TO_CHAR(timezone('Asia/Amman', "
            "timezone('UTC', date_order)), 'YYYY-mm') = "
            f"'{current_date.strftime('%Y-%m')}';"
        )
        stdin, stdout, stderr = ssh.exec_command(
            f'su postgres -c "psql -d {server} -t -A -F"," -c \\"{sql_query}\\""'
        )
        logger.info("stdin data %s...", stdin)
        logger.info("stdin data %s...", stderr)
        result_str = stdout.read().decode().strip()
        result = float(result_str) if result_str else 0

        existing_record_index = (
            (result_df["Year"] == current_date.year)
            & (result_df["Month"] == current_date.month)
            & (result_df["Server"] == server)
        )

        if not result_df[existing_record_index].empty:
            result_df.loc[existing_record_index, "Total Amount"] += (
                result if result else 0
            )
        else:
            new_dic = {
                "Year": current_date.year,
                "Month": current_date.month,
                "Total Amount": result,
                "Server": server,
            }
            result_df = pd.concat(
                [result_df, pd.DataFrame([new_dic])], ignore_index=True
            )
        return result_df

    def process_month_data(
        self,
        ssh: paramiko.server,
        server: str,
        current_date: datetime,
        result_df: pd.DataFrame,
        table_name: str,
    ):
        if table_name == "pos_order":
            row_name = "Total # Transactions POS"
        elif table_name == "sale_order":
            row_name = "Total # Transactions SO"
        elif table_name == "purchase_order":
            row_name = "Total # Transactions PO"

        tz_data = "'Asia/Amman', timezone('UTC', po.date_order)), 'YYYY-MM'"
        sql_query = f"""
                SELECT COUNT( DISTINCT po.name) as order_count
                FROM {table_name} as po
                WHERE
                TO_CHAR(timezone({tz_data}) =
                 '{current_date.strftime('%Y-%m')}'
                    AND po.state NOT IN ('cancel', 'draft')"""

        stdin, stdout, stderr = ssh.exec_command(
            f'su postgres -c "psql -d {server} -t -A -F"," -c \\"{sql_query}\\""'
        )
        logger.info("stdin data %s...", stdin)
        logger.info("stdin data %s...", stderr)

        result_str = stdout.read().decode().strip()
        result = float(result_str) if result_str else 0
        existing_record_index = (
            (result_df["Year"] == current_date.year)
            & (result_df["Month"] == current_date.month)
            & (result_df["Server"] == server)
        )
        if not result_df[existing_record_index].empty:
            result_df.loc[existing_record_index, row_name] = result or 0
        else:
            new_dic = {
                "Year": current_date.year,
                "Month": current_date.month,
                "row_name": result or 0,
                "Server": server,
            }
            result_df = pd.concat(
                [result_df, pd.DataFrame([new_dic])], ignore_index=True
            )
        return result_df
