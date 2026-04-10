from target_netsuite.netsuite.utils import stringify_number
import logging

logger = logging.getLogger("target-netsuite")
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

def _normalize_lookup_value(value):
    return str(value).strip().lower()


def _resolve_select_option_internal_id(script_id, value, options_by_internalId):
    if not options_by_internalId:
        return None

    lookup_key = _normalize_lookup_value(value)
    for internal_id, option_name in options_by_internalId.items():
        if lookup_key == option_name:
            #get the first match (name is not unique)
            return internal_id

    # NetSuite can return parent-qualified labels (e.g. "online : instagram").
    leaf_key = lookup_key.split(":")[-1].strip()
    matching_internal_ids = []
    for internal_id, option_name in options_by_internalId.items():
        option_leaf = option_name.split(":")[-1].strip()
        if option_leaf == leaf_key:
            matching_internal_ids.append(internal_id)

    if len(matching_internal_ids) == 1:
        return matching_internal_ids[0]

    if len(matching_internal_ids) > 1:
        raise ValueError(
            f"Custom field '{script_id}' value '{value}' is ambiguous. "
            "Provide the full parent-qualified option label (for example, 'parent : child')."
        )

    return None


def _get_select_value_page(ns_client, field_description, max_pages=50):
    all_values = []
    for page_index in range(1, max_pages):
        try:
            res = ns_client.client.request(
                "getSelectValue",
                fieldDescription=field_description,
                pageIndex=page_index,
            )
            result = getattr(res, "body", None)
            result = getattr(result, "getSelectValueResult", result)
            total_pages = getattr(result, "totalPages", max_pages)
            base_ref_list = getattr(result, "baseRefList", None)
            base_refs = getattr(base_ref_list, "baseRef", None) if base_ref_list is not None else None
            page_values = []
            for ref in base_refs or []:
                ref_values = _extract_values(ref)
                if not isinstance(ref_values, dict):
                    continue
                internal_id = ref_values.get("internalId")
                name = ref_values.get("name")
                if internal_id and name:
                    page_values.append({"internalId": str(internal_id), "name": str(name)})
            all_values.extend(page_values)
            if not page_values or total_pages <= page_index:
                break
        except Exception as exc:
            logger.debug(f"Failed getSelectValue request for {field_description} page {page_index}: {exc}")
            break
    return all_values

def _get_select_options_by_internalId(ns_client, script_id):
    field_descriptions = [
        {"recordType": "journalEntry", "sublist": "lineList", "field": script_id},
        {"recordType": "journalEntry", "field": script_id},
    ]
    for field_description in field_descriptions:
        collected = _get_select_value_page(ns_client, field_description)

        if collected:
            options_by_internalId = {}
            for entry in collected:
                options_by_internalId[str(entry["internalId"])] = _normalize_lookup_value(entry["name"])
            return options_by_internalId

    return {}


def _extract_values(obj):
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "__dict__"):
        return getattr(obj, "__dict__", {}).get("__values__", {}) or {}
    return {}


def _extract_customization_refs(result_obj):
    body = getattr(result_obj, "body", None)
    result = getattr(body, "getCustomizationIdResult", body)
    ref_list = getattr(result, "customizationRefList", None)
    refs = getattr(ref_list, "customizationRef", None) if ref_list is not None else None
    out = []
    for ref in refs or []:
        out.append(_extract_values(ref))
    return out


def _find_custom_record_type_id_via_get_customization_id(ns_client, custom_record_type_script_id):
    attempts = [
        {"customizationType": "customRecordType", "includeInactives": False},
        {"getCustomizationType": "customRecordType", "includeInactives": False},
        {"customizationType": "customRecordType"},
        {"getCustomizationType": "customRecordType"},
    ]

    for payload in attempts:
        try:
            res = ns_client.client.request("getCustomizationId", **payload)
            refs = _extract_customization_refs(res)
            if refs:
                for ref in refs:
                    script_id = str(ref.get("scriptId") or "").lower()
                    if script_id == custom_record_type_script_id.lower():
                        internal_id = ref.get("internalId")
                        if internal_id:
                            logger.debug(
                                f"Resolved custom record type '{custom_record_type_script_id}' "
                                f"to internalId={internal_id} via getCustomizationId"
                            )
                            return str(internal_id)
        except Exception as exc:
            logger.debug(f"getCustomizationId attempt failed ({payload}): {exc}")

    return None


def _soap_search_all(ns_client, search_record):
    records_out = []
    try:
        res = ns_client.client.request("search", searchRecord=search_record)
    except Exception as exc:
        logger.debug(f"SOAP search failed on first page: {exc}")
        return records_out

    body = getattr(res, "body", None)
    result = getattr(body, "searchResult", body)
    record_list = getattr(result, "recordList", None)
    first_records = getattr(record_list, "record", None) if record_list is not None else None
    for r in first_records or []:
        records_out.append(_extract_values(r))

    search_id = getattr(result, "searchId", None)
    total_pages = int(getattr(result, "totalPages", 1) or 1)

    if not search_id or total_pages <= 1:
        return records_out

    for page_index in range(2, total_pages + 1):
        try:
            page_res = ns_client.client.request(
                "searchMoreWithId",
                searchId=search_id,
                pageIndex=page_index,
            )
            page_body = getattr(page_res, "body", None)
            page_result = getattr(page_body, "searchResult", page_body)
            page_record_list = getattr(page_result, "recordList", None)
            page_records = getattr(page_record_list, "record", None) if page_record_list is not None else None
            for r in page_records or []:
                records_out.append(_extract_values(r))
        except Exception as exc:
            logger.debug(f"SOAP searchMoreWithId failed (page={page_index}): {exc}")
            break

    return records_out


def _search_custom_records_for_rec_type_ref(ns_client, rec_type_ref, segment_script_id):
    try:
        search_record = ns_client.client.basic_search_factory(
            type_name="CustomRecord",
            recType=rec_type_ref,
        )
    except Exception as exc:
        logger.debug(f"Could not build CustomRecord search for segment '{segment_script_id}': {exc}")
        return {}

    records = _soap_search_all(ns_client, search_record)
    options_by_internalId = {}
    for rec in records:
        name = rec.get("name")
        internal_id = rec.get("internalId")
        if name and internal_id:
            options_by_internalId[str(internal_id)] = _normalize_lookup_value(name)

    return options_by_internalId


def _get_segment_options_via_custom_record_search(ns_client, segment_script_id):
    custom_record_type_script_id = f"customrecord_{segment_script_id.lower()}"
    rec_type_internal_id = _find_custom_record_type_id_via_get_customization_id(
        ns_client, custom_record_type_script_id
    )
    if rec_type_internal_id:
        options_by_internalId = _search_custom_records_for_rec_type_ref(
            ns_client,
            ns_client.client.RecordRef(internalId=rec_type_internal_id),
            segment_script_id,
        )
        if options_by_internalId:
            logger.info(
                f"Custom Segment '{segment_script_id}' fetched "
                f"{len(options_by_internalId)} options via CustomRecord recType internalId={rec_type_internal_id}"
            )
            return options_by_internalId

    logger.debug(f"Custom Segment returned no value for '{segment_script_id}'")
    return {}


def _get_lookup_options_for_custom_field(ns_client, script_id):
    script_id_lower = script_id.lower() if isinstance(script_id, str) else ""

    if script_id_lower.startswith("custbody") or script_id_lower.startswith("custcol"):
        return _get_select_options_by_internalId(ns_client, script_id)

    elif script_id_lower.lower().startswith("cseg"):
        return _get_segment_options_via_custom_record_search(ns_client, script_id)

    return {}


def prepare_custom_field_lookups(ns_client, input_data, config):
    custom_fields = config.get("custom_fields") or []
    custom_field_lookup = {}

    for entry in custom_fields:
        input_id = entry.get("input_id")
        script_id = entry.get("netsuite_id")
        if not input_id or not script_id:
            continue

        if input_id not in input_data.columns:
            continue

        values = [v for v in input_data[input_id].dropna().tolist() if str(v).strip() != ""]
        if not values:
            continue

        options_by_internalId = _get_lookup_options_for_custom_field(ns_client, script_id)

        if not options_by_internalId:
            # Not all configured fields are List/Record selects.
            # If options are unavailable, keep legacy passthrough behavior.
            logger.debug(
                f"Lookup unavailable for custom field '{script_id}', using passthrough values."
            )
            continue

        unique_missing = sorted({
            str(value)
            for value in values
            if (
                not _is_internal_id_value(value, options_by_internalId)
                and _resolve_select_option_internal_id(script_id, value, options_by_internalId) is None
            )
        })
        if unique_missing:
            raise ValueError(
                f"Invalid value(s) for custom field '{script_id}': {unique_missing}. "
                f"Provide a valid option label."
            )

        custom_field_lookup[script_id] = options_by_internalId
        logger.info(
            f"Lookup enabled for custom field '{script_id}' "
            f"({len(values)} value(s) checked, {len(options_by_internalId)} option(s) cached)."
        )

    return custom_field_lookup



def _is_internal_id_value(value, options_by_internalId):
    if value is None or not options_by_internalId:
        return False

    try:
        internal_id = _to_internal_id_string(value)
    except Exception:
        return False

    return internal_id in options_by_internalId


def _to_internal_id_string(value):
    if isinstance(value, str):
        value = value.strip()
    return stringify_number(value)

def resolve_custom_field_value(script_id, value, config):
    options_by_internalId = (config.get("_custom_field_lookup") or {}).get(script_id, {})
    # No lookup table for this field: preserve original value.
    # Example: Free-Form Text at netsuite
    if not options_by_internalId:
        return value
        
    resolved_internal_id = None
    if _is_internal_id_value(value, options_by_internalId):
        resolved_internal_id =  _to_internal_id_string(value)
    else:
        resolved_internal_id = _resolve_select_option_internal_id(script_id, value, options_by_internalId)
    
    if resolved_internal_id is not None:
        return resolved_internal_id
    
    raise ValueError(
        f"Custom field '{script_id}' received value '{value}' that could not be resolved to an internalId."
    )