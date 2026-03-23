import asyncio
import json
from typing import List, Union

import aiohttp
import frappe
import frappe.defaults
from frappe import _
from frappe.query_builder import DocType


from ..doctype.doctype_names_mapping import (
    DIGITAX_ID_MAPPING_DOCTYPE_NAME,
    SETTINGS_DOCTYPE_NAME,
)
from ..utils import (
    build_item_payload,
    build_return_invoice_payload,
    chunked,
    get_active_settings,
    get_invoice_reference_number,
    make_get_request,
)
from .api_builder import EndpointsBuilder
from .process_request import process_request
from .remote_response_status_handlers import (
    customer_details_submission_on_success,
    customer_details_submission_on_error,
    customers_search_on_success,
    item_registration_on_success,
    item_search_on_success,
    process_invoice_response,
    sales_information_submission_on_success,
    update_invoice_info,
    verify_and_fix_invoice_info,
)

endpoints_builder = EndpointsBuilder()


def _parse_docs_list(docs_list: Union[str, list, tuple]) -> list:
    """Parse docs_list that may be a JSON-encoded string or an already-decoded list/tuple."""
    if isinstance(docs_list, (list, tuple)):
        return list(docs_list)
    if isinstance(docs_list, str):
        try:
            return json.loads(docs_list)
        except json.JSONDecodeError:
            frappe.throw(_("Invalid docs_list format. Expected a JSON array or list of names."))
    frappe.throw(_("Invalid docs_list type. Expected a JSON string or list of names."))


@frappe.whitelist()
def bulk_submit_sales_invoices(docs_list: Union[str, list, tuple, None] = None, settings_name: str = None) -> str:
    """Bulk submit sales invoices in chunks"""
    filters = {"docstatus": 1, "successfully_submitted": 0}

    if docs_list:
        provided_names = _parse_docs_list(docs_list)
        valid_invoices = frappe.get_all("Sales Invoice", filters=filters, pluck="name")
        invoices_to_process = [n for n in provided_names if n in valid_invoices]
    else:
        invoices_to_process = frappe.get_all(
            "Sales Invoice", filters=filters, pluck="name"
        )

    if not invoices_to_process:
        return "No invoices to process."

    for batch in chunked(invoices_to_process, 100):
        frappe.enqueue(
            process_invoices_sequentially,
            invoice_list=batch,
            queue="long",
            timeout=3600,
            enqueue_after_commit=True,
            job_name=f"Bulk Submit Invoices Batch ({len(batch)})",
        )

    return "Processing started."


def process_invoices_sequentially(invoice_list: List[str]) -> None:
    """Process a batch of invoices sequentially"""
    from ..overrides.server.sales_invoice import on_submit

    for name in invoice_list:
        try:
            doc = frappe.get_doc("Sales Invoice", name)
            on_submit(doc)
            frappe.db.commit()
        except Exception:
            frappe.db.rollback()
            frappe.log_error(f"Bulk Submit Error: {name}", frappe.get_traceback())
            continue


@frappe.whitelist()
def bulk_verify_and_resend_invoices(docs_list: Union[str, list, tuple], settings_name: str = None) -> None:
    """Bulk verify and resend invoices in chunks"""
    invoices_to_process = []

    if docs_list:
        data = _parse_docs_list(docs_list)
        all_sales_invoices = frappe.db.get_all(
            "Sales Invoice", {"docstatus": 1}, ["name"]
        )

        for record in data:
            for invoice in all_sales_invoices:
                if record == invoice.name:
                    invoices_to_process.append(record)
    else:
        all_invoices = frappe.db.get_all("Sales Invoice", {"docstatus": 1}, ["name"])
        invoices_to_process = [invoice.name for invoice in all_invoices]

    for batch in chunked(invoices_to_process, 100):
        frappe.enqueue(
            process_verify_invoice_batch,
            invoice_names=batch,
            settings_name=settings_name,
            queue="long",
            job_name=f"Verify Invoices Batch ({len(batch)})",
        )


def process_verify_invoice_batch(
    invoice_names: List[str], settings_name: str = None
) -> None:
    """Process a batch of invoice verifications"""
    for invoice_name in invoice_names:
        doc = frappe.get_doc("Sales Invoice", invoice_name, for_update=False)
        frappe.enqueue(
            verify_invoice_details,
            id=None,
            document_name=doc.name,
            invoice_type="Sales Invoice",
            settings_name=settings_name,
            company=doc.company,
        )


@frappe.whitelist()
def bulk_register_items(docs_list: Union[str, list, tuple], settings_name: str = None) -> None:
    """Bulk register items in chunks"""
    item_names = _parse_docs_list(docs_list)
    settings = (
        [frappe.get_doc(SETTINGS_DOCTYPE_NAME, settings_name)]
        if settings_name
        else get_active_settings()
    )

    if not item_names or not settings:
        return

    for setting in settings:
        for batch in chunked(item_names, 100):
            frappe.enqueue(
                process_item_batch,
                queue="long",
                settings_name=setting.name,
                items=batch,
                job_name=f"Item Register Batch ({len(batch)})",
            )


@frappe.whitelist()
def update_all_items(settings_name: str = None) -> None:
    """Update all items in chunks"""
    settings = (
        [frappe.get_doc(SETTINGS_DOCTYPE_NAME, settings_name)]
        if settings_name
        else get_active_settings()
    )

    if not settings:
        return

    for setting in settings:
        Item = DocType("Item")
        Mapping = DocType(DIGITAX_ID_MAPPING_DOCTYPE_NAME)

        items = (
            frappe.qb.from_(Item)
            .left_join(Mapping)
            .on(
                (Mapping.parent == Item.name)
                & (Mapping.parenttype == "Item")
                & (Mapping.etims_setup == setting.name)
            )
            .select(Item.name)
            .where(Mapping.name.isnull())
            .run(as_dict=True)
        )

        item_names = [i.name for i in items]

        for batch in chunked(item_names, 100):
            frappe.enqueue(
                process_item_batch,
                queue="long",
                settings_name=setting.name,
                items=batch,
                job_name=f"Item Update Batch ({len(batch)})",
            )


@frappe.whitelist()
def register_all_items(settings_name: str = None) -> None:
    """Register all items in chunks"""
    settings = (
        [frappe.get_doc(SETTINGS_DOCTYPE_NAME, settings_name)]
        if settings_name
        else get_active_settings()
    )

    if not settings:
        return

    for setting in settings:
        Item = DocType("Item")
        Mapping = DocType(DIGITAX_ID_MAPPING_DOCTYPE_NAME)

        items = (
            frappe.qb.from_(Item)
            .left_join(Mapping)
            .on(
                (Mapping.parent == Item.name)
                & (Mapping.parenttype == "Item")
                & (Mapping.etims_setup == setting.name)
            )
            .select(Item.name)
            .where(Mapping.name.isnull())
            .run(as_dict=True)
        )

        item_names = [i.name for i in items]

        for batch in chunked(item_names, 100):
            frappe.enqueue(
                process_item_batch,
                queue="long",
                settings_name=setting.name,
                items=batch,
                job_name=f"Item Register Batch ({len(batch)})",
            )


def process_item_batch(settings_name: str, items: List[str]) -> None:
    """Process a batch of items for registration/update"""
    for item_name in items:
        perform_item_registration(
            item_name=item_name,
            settings_name=settings_name,
        )


@frappe.whitelist()
def perform_item_registration(item_name: str, settings_name: str) -> dict | None:
    """Main function to handle item registration with Digitax, including validation and auto-filling of fields."""
    from ..overrides.server.item import autofill_item_etims_fields

    item = frappe.get_doc("Item", item_name)

    if not is_item_eligible_for_registration(item):
        return None

    defaults = autofill_item_etims_fields(
        item_group=item.item_group,
        settings_name=settings_name,
    )

    updates = {}

    for field in validate_required_fields(item):
        if defaults.get(field):
            updates[field] = defaults.get(field)

    if updates:
        frappe.db.set_value("Item", item.name, updates, update_modified=True)
        for k, v in updates.items():
            item.set(k, v)

    missing_fields = validate_required_fields(item)
    if missing_fields:
        frappe.throw(
            _("Missing required ETIMS fields: {0}").format(
                ", ".join(
                    frappe.bold(field.replace("_", " ").title())
                    for field in missing_fields
                )
            )
        )

    request_data = build_item_payload(item)

    frappe.enqueue(
        process_request,
        queue="default",
        is_async=True,
        request_data=request_data,
        route_key="RegisterItemReq",
        handler_function=item_registration_on_success,
        request_method="POST",
        doctype="Item",
        settings_name=settings_name,
    )


@frappe.whitelist(allow_guest=True)
def item_registration_callback(**kwargs) -> None:
    try:
        data = kwargs.get("data", {})
        digitax_id = data.get("id")
        etims_code = data.get("etims_item_code")

        if not digitax_id:
            return

        mapping_name = frappe.db.get_value(
            DIGITAX_ID_MAPPING_DOCTYPE_NAME,
            {
                "parenttype": "Item",
                "digitax_id": digitax_id,
            },
            "name",
        )

        if mapping_name:
            frappe.db.set_value(
                DIGITAX_ID_MAPPING_DOCTYPE_NAME,
                mapping_name,
                {
                    "etims_code": etims_code,
                    "sent_to_etims": 1,
                    "digitax_id": digitax_id,
                    "disabled": 0,
                },
            )
            frappe.db.commit()

    except Exception:
        frappe.log_error(
            title="Item Registration Callback Error", message=frappe.get_traceback()
        )


@frappe.whitelist(allow_guest=True)
def invoice_submission_callback(**kwargs) -> None:
    try:
        data = kwargs.get("data")
        if not data:
            return
        process_invoice_response(
            response=data,
            doctype="Sales Invoice",
            document_name=data.get("trader_invoice_number"),
        )

    except Exception:
        frappe.log_error(
            title="Invoice Submission Callback Error",
            message=frappe.get_traceback(),
        )


def is_item_eligible_for_registration(item) -> bool:
    """Check if item meets basic registration criteria"""
    return not (item.prevent_etims_registration or item.disabled)


def validate_required_fields(item) -> List[str]:
    """Validate required fields for item registration"""
    required_fields = [
        "etims_country_of_origin",
        "product_type",
        "item_type",
        "etims_country_of_origin",
        "packaging_unit",
        "unit_of_quantity",
        "taxation_type",
    ]
    return [field for field in required_fields if not item.get(field)]


@frappe.whitelist()
def fetch_item_details(request_data: str, settings_name: str) -> None:
    """Fetch item details"""
    process_request(
        request_data,
        "ItemSearchReq",
        item_search_on_success,
        doctype="Item",
        settings_name=settings_name,
    )


@frappe.whitelist()
def bulk_submit_customers(docs_list: Union[str, list, tuple], settings_name: str = None) -> None:
    """Bulk submit customers in chunks"""
    customers = _parse_docs_list(docs_list)
    settings = (
        [frappe.get_doc(SETTINGS_DOCTYPE_NAME, settings_name)]
        if settings_name
        else get_active_settings()
    )
    if not customers or not settings:
        return

    for setting in settings:
        for batch in chunked(customers, 100):
            frappe.enqueue(
                process_customer_batch,
                queue="long",
                settings_name=setting.name,
                customers=batch,
                job_name=f"Bulk Submit Customers Batch ({len(batch)})",
            )


@frappe.whitelist()
def submit_all_customers(settings_name: str = None) -> None:
    """Submit all customers in chunks"""
    active_settings = (
        [frappe.get_doc(SETTINGS_DOCTYPE_NAME, settings_name)]
        if settings_name
        else get_active_settings()
    )

    if not active_settings:
        return

    for setting in active_settings:
        Customer = DocType("Customer")
        Mapping = DocType(DIGITAX_ID_MAPPING_DOCTYPE_NAME)

        query = (
            frappe.qb.from_(Customer)
            .left_join(Mapping)
            .on(
                (Mapping.parent == Customer.name)
                & (Mapping.parenttype == "Customer")
                & (Mapping.etims_setup == setting.name)
            )
            .select(Customer.name)
            .where(Mapping.name.isnull())
        )

        customers = query.run(as_dict=True)
        customer_names = [c.name for c in customers]

        for batch in chunked(customer_names, 100):
            frappe.enqueue(
                process_customer_batch,
                queue="long",
                settings_name=setting.name,
                customers=batch,
                job_name=f"Customer Submit Batch ({len(batch)})",
            )


def process_customer_batch(settings_name: str, customers: List[str]) -> None:
    """Process a batch of customers"""
    for customer in customers:
        send_customer_details(
            settings_name=settings_name,
            name=customer,
        )


@frappe.whitelist()
def send_customer_details(name: str, settings_name: str) -> None:
    doctype = "Customer"
    data = frappe.get_doc(doctype, name)

    if (hasattr(data, "disabled") and data.disabled) or (
        hasattr(data, "prevent_etims_registration") and data.prevent_etims_registration
    ):
        return

    request_data = {
        "customer_name": data.customer_name if hasattr(data, "customer_name") else name,
        "customer_tin": data.tax_id if hasattr(data, "tax_id") else None,
        "document_name": name,
    }

    if hasattr(data, "email") and data.email:
        request_data["email"] = data.email

    if hasattr(data, "phone") and data.phone:
        request_data["phone"] = data.phone

    frappe.enqueue(
        process_request,
        queue="default",
        is_async=True,
        request_data=request_data,
        route_key="CustSaveReq",
        handler_function=customer_details_submission_on_success,
        error_callback=customer_details_submission_on_error,
        request_method="POST",
        doctype=doctype,
        settings_name=settings_name,
    )


@frappe.whitelist()
def get_customer_details(
    request_data: str,
    settings_name: str,
) -> None:
    """Get customer details"""
    return process_request(
        request_data,
        "CustomerSearchReq",
        customers_search_on_success,
        settings_name=settings_name,
    )


@frappe.whitelist()
def perform_item_search(request_data: str, settings_name: str) -> None:
    """Perform item search"""
    process_request(
        request_data,
        "ItemsSearchReq",
        item_search_on_success,
        doctype="Item",
        settings_name=settings_name,
    )


@frappe.whitelist()
def ping_server(request_data: str) -> None:
    """Ping the server"""
    data = json.loads(request_data)
    server_url = data.get("server_url")
    auth_url = data.get("auth_url")

    async def check_server(url: str) -> tuple:
        try:
            response = await make_get_request(url)
            return "Online", response
        except aiohttp.client_exceptions.ClientConnectorError:
            return "Offline", None

    async def main() -> None:
        server_status, server_response = await check_server(server_url)
        auth_status, auth_response = await check_server(auth_url)

        if server_response:
            frappe.msgprint(f"Server Status: {server_status}\n{server_response}")
        else:
            frappe.msgprint(f"Server Status: {server_status}")

        frappe.msgprint(f"Auth Server Status: {auth_status}")

    asyncio.run(main())


@frappe.whitelist()
def _process_invoice_fetch_request(
    id: str = None,
    document_name: str = None,
    invoice_type: str = "Sales Invoice",
    settings_name: str = None,
    company: str = None,
    handler_function=None,
    reference_number: str = None,
    is_return: bool = False,
    original_invoice_id: str = None,
) -> None:
    """Common helper function to process invoice-related requests."""
    invoice = frappe.get_doc(invoice_type, document_name)

    if is_return and not original_invoice_id:
        frappe.throw("Original invoice ID is required for return processing.")

    request_data = {
        "document_name": document_name,
        "company": company or invoice.company,
    }

    route_key = "TrnsSalesSearchReq"

    if invoice.is_return or is_return:
        route_key = "SalesCreditNoteSaveReq"

    if id:
        request_data["id"] = id
    else:
        if (invoice.is_return and invoice.return_against) or (
            is_return and original_invoice_id
        ):
            route_key = "SalesCreditNoteSaveReq"
            original_invoice_digitax_id = (
                original_invoice_id
                if is_return
                else frappe.db.get_value(
                    "Sales Invoice", invoice.return_against, "digitax_id"
                )
            )
            request_data["invoice"] = original_invoice_digitax_id
        else:
            route_key = "CreditNoteSaveReq"
            request_data["reference_number"] = reference_number

    return process_request(
        request_data,
        route_key,
        handler_function,
        doctype=invoice_type,
        settings_name=settings_name,
        company=company,
    )


@frappe.whitelist()
def get_invoice_details(
    id: str = None,
    document_name: str = None,
    invoice_type: str = "Sales Invoice",
    settings_name: str = None,
    company: str = None,
) -> None:
    """Get invoice details"""
    invoice = frappe.get_doc(invoice_type, document_name)
    reference_number = get_invoice_reference_number(invoice)
    _process_invoice_fetch_request(
        id=None,
        document_name=document_name,
        invoice_type=invoice_type,
        settings_name=settings_name,
        company=company,
        handler_function=update_invoice_info,
        reference_number=reference_number,
    )


@frappe.whitelist()
def verify_invoice_details(
    id: str = None,
    document_name: str = None,
    invoice_type: str = "Sales Invoice",
    settings_name: str = None,
    company: str = None,
) -> None:
    """Verify invoice details"""
    invoice = frappe.get_doc(invoice_type, document_name)
    reference_number = get_invoice_reference_number(invoice)
    _process_invoice_fetch_request(
        id=id,
        document_name=document_name,
        invoice_type=invoice_type,
        settings_name=settings_name,
        company=company,
        handler_function=verify_and_fix_invoice_info,
        reference_number=reference_number,
    )


@frappe.whitelist()
def submit_credit_note(
    response: dict, document_name: str, doctype: str, settings_name: str, **kwargs
) -> None:
    """Submit credit note"""
    doc = frappe.get_doc(doctype, document_name)
    data = response.get("results", [])[0] if response.get("results") else response
    scu_data = data.get("scu_data")
    if not scu_data:
        return
    payload = build_return_invoice_payload(doc, data)
    frappe.enqueue(
        process_request,
        queue="default",
        is_async=True,
        request_data=payload,
        route_key="CreditNoteSaveReq",
        handler_function=sales_information_submission_on_success,
        request_method="POST",
        doctype=doctype,
        settings_name=settings_name,
        company=doc.company,
    )
