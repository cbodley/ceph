// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef RGW_ERROR_CODE_H
#define RGW_ERROR_CODE_H

#include <string>

#include "common/error_code.h"
#include "rgw_common.h"
#include "rgw_error_code.h"

class rgw_error_category : public ceph::converting_category {
public:
  rgw_error_category(){}
  const char* name() const noexcept override;
  std::string message(int ev) const noexcept override;
  boost::system::error_condition default_error_condition(int ev) const noexcept
    override;
  bool equivalent(int ev, const boost::system::error_condition& c) const
    noexcept override;
  using ceph::converting_category::equivalent;
  int from_code(int ev) const noexcept override;
};

const char* rgw_error_category::name() const noexcept {
  return "monc";
}

std::string rgw_error_category::message(int ev) const noexcept {
  using namespace ::rgw_errc;


  switch (ev) {
  case 0:
    return "No error";

  case zonegroup_missing_zone:
    return "Zonegroup missing zone";
  case cannot_find_zone:
    return "Cannot find zone";
  case tier_type_not_found:
    return "Tier type not found";
  case invalid_bucket_name:
    return "Invalid bucket name";
  case invalid_object_name:
    return "Invalid object name.";
  case no_such_bucket:
    return "No such bucket";
  case method_not_allowed:
    return "Method not allowed";
  case invalid_digest:
    return "Invalid digest.";
  case bad_digest:
    return "Bad digest.";
  case unresolvable_email:
    return "Unresolvable email.";
  case invalid_part:
    return "Invalid part.";
  case invalid_part_order:
    return "Invalid part order.";
  case no_such_upload:
    return "No such upload";
  case request_timeout:
    return "Request timeout";
  case length_required:
    return "Length required";
  case request_time_skewed:
    return "Request time skewed";
  case bucket_exists:
    return "Bucket exists";
  case bad_url:
    return "Bad URL";
  case precondition_failed:
    return "Precondition failed";
  case not_modified:
    return "Not modified.";
  case invalid_utf8:
    return "Invalid UTF-8";
  case unprocessable_entity:
    return "Unprocessable entity";
  case too_large:
    return "Too large";
  case too_many_buckets:
    return "Too many buckets";
  case invalid_request:
    return "Invalid request";
  case too_small:
    return "Too small";
  case not_found:
    return "Not found";
  case permanent_redirect:
    return "Permanent redirect";
  case locked:
    return "Locked";
  case quota_exceeded:
    return "Quota exceeded";
  case signature_no_match:
    return "Signature no match";
  case invalid_access_key:
    return "Invalid access key";
  case malformed_xml:
    return "Malformed XML";
  case user_exist:
    return "User exists";
  case not_slo_manifest:
    return "Not SLO manifest";
  case email_exist:
    return "Email exists";
  case key_exist:
    return "Key exists";
  case invalid_secret_key:
    return "Invalid secret key";
  case invalid_key_type:
    return "Invalid key type";
  case invalid_cap:
    return "Invalid cap";
  case invalid_tenant_name:
    return "Invalid tenant name";
  case website_redirect:
    return "Website redirect";
  case no_such_website_configuration:
    return "No such website configuration";
  case amz_content_sha256_mismatch:
    return "amz_content_sha256 mismatch";
  case no_such_lc:
    return "No such lifecycle.";
  case no_such_user:
    return "No such user";
  case no_such_subuser:
    return "No such subuser";
  case mfa_required:
    return "MFA required";
  case no_such_cors_configuration:
    return "No such CORS configuration";
  case user_suspended:
    return "User suspended";
  case internal_error:
    return "Internal error";
  case not_implemented:
    return "Not implemented";
  case service_unavailable:
    return "Service unavailable";
  case role_exists:
    return "Role Exists";
  case malformed_doc:
    return "Malformed document";
  case no_role_found:
    return "No role found";
  case delete_conflict:
    return "Delete conflict";
  case no_such_bucket_policy:
    return "No such bucket policy";
  case invalid_location_constraint:
    return "Invalid location constraint";
  case tag_conflict:
    return "Tag conflict";
  case invalid_tag:
    return "Invalid tag";
  case zero_in_url:
    return "Zero in URL";
  case malformed_acl_error:
    return "Malformed ACL error";
  case zonegroup_default_placement_misconfiguration:
    return "Zonegroup default placement misconfiguration";
  case invalid_encryption_algorithm:
    return "Invalid encryption algorithm";
  case invalid_cors_rules_error:
    return "Invalid CORS rules error";
  case no_cors_found:
    return "No CORS found";
  case invalid_website_routing_rules_error:
    return "Invalid website routing rules error";
  case rate_limited:
    return "Rate limited";
  case position_not_equal_to_length:
    return "Position not equal to length";
  case object_not_appendable:
    return "Object not appendable";
  case invalid_bucket_state:
    return "Invalid bucket state";
  case busy_resharding:
    return "Busy resharding";
  case no_such_entity:
    return "No such entity";
  case packed_policy_too_large:
    return "Packed policy too large";
  case invalid_identity_token:
    return "Invalid identity token";
  case user_not_permitted_to_use_placement_rule:
    return "User not permitted to use placement rule";
  case zone_does_not_contain_placement_rule_present_in_zone_group:
    return "Zone does not contain placement rule present in zone group";
  case storage_class_dne:
    return "Requested storage class does not exist";
  case requirement_exceeds_limit:
    return "Requirement for operation exceeds allowed capacity";
  case placement_pool_missing:
    return "Placement pool missing";
  }

  return "Unknown error";
}

boost::system::error_condition rgw_error_category::default_error_condition(int ev) const noexcept {
  using namespace ::rgw_errc;
  switch (ev) {
  case zonegroup_missing_zone:
  case cannot_find_zone:
  case tier_type_not_found:
  case invalid_bucket_name:
  case invalid_object_name:
  case method_not_allowed:
  case invalid_digest:
  case bad_digest:
  case unresolvable_email:
  case invalid_part:
  case invalid_part_order:
  case length_required:
  case request_time_skewed:
  case bad_url:
  case invalid_utf8:
  case unprocessable_entity:
  case too_large:
  case invalid_request:
  case too_small:
  case signature_no_match:
  case invalid_access_key:
  case malformed_xml:
  case not_slo_manifest:
  case invalid_secret_key:
  case invalid_key_type:
  case invalid_cap:
  case invalid_tenant_name:
  case packed_policy_too_large:
  case invalid_identity_token:
  case malformed_doc:
  case invalid_location_constraint:
  case invalid_tag:
  case zero_in_url:
  case invalid_website_routing_rules_error:
  case invalid_bucket_state:
  case object_not_appendable:
  case position_not_equal_to_length:
  case zone_does_not_contain_placement_rule_present_in_zone_group:
    return boost::system::errc::invalid_argument;

  case no_such_bucket:
  case no_such_upload:
  case not_found:
  case no_such_website_configuration:
  case no_such_lc:
  case no_such_user:
  case no_such_subuser:
  case no_such_cors_configuration:
  case no_role_found:
  case no_such_bucket_policy:
  case no_cors_found:
  case no_such_entity:
  case storage_class_dne:
  case placement_pool_missing:
    return ceph::errc::does_not_exist;

  case request_timeout:
    return boost::system::errc::timed_out;

  case bucket_exists:
  case not_modified:
  case permanent_redirect:
  case user_exist:
  case email_exist:
  case key_exist:
  case website_redirect:
  case role_exists:
    return ceph::errc::exists;

  case too_many_buckets:
  case quota_exceeded:
    return ceph::errc::limit_exceeded;

  case locked:
  case service_unavailable:
  case busy_resharding:
  case rate_limited:
    return boost::system::errc::resource_unavailable_try_again;

  case amz_content_sha256_mismatch:
  case mfa_required:
  case user_suspended:
  case malformed_acl_error:
  case invalid_encryption_algorithm:
  case invalid_cors_rules_error:
    return ceph::errc::auth;

  case internal_error:
  case zonegroup_default_placement_misconfiguration:
    return ceph::errc::failure;

  case not_implemented:
    return boost::system::errc::operation_not_supported;

  case precondition_failed:
  case delete_conflict:
  case tag_conflict:
    return ceph::errc::conflict;

  case user_not_permitted_to_use_placement_rule:
    return boost::system::errc::operation_not_permitted;

  case requirement_exceeds_limit:
    return boost::system::errc::resource_deadlock_would_occur;

  }
  return { ev, *this };
}

bool rgw_error_category::equivalent(int ev, const boost::system::error_condition& c) const noexcept {
  using namespace ::rgw_errc;
  switch (ev) {
  case malformed_acl_error:
  case invalid_encryption_algorithm:
  case invalid_cors_rules_error:
    return c == boost::system::errc::invalid_argument;

  case requirement_exceeds_limit:
    return c == ceph::errc::limit_exceeded;

  case placement_pool_missing:
    return c == boost::system::errc::io_error;
  }
  return default_error_condition(ev) == c;
}

int rgw_error_category::from_code(int ev) const noexcept {
  using namespace ::rgw_errc;
  switch (ev) {
  case 0:
    return 0;
  case zonegroup_missing_zone:
  case cannot_find_zone:
  case tier_type_not_found:
  case zone_does_not_contain_placement_rule_present_in_zone_group:
  case storage_class_dne:
    return -EINVAL;
  case invalid_bucket_name:
    return -ERR_INVALID_BUCKET_NAME;
  case invalid_object_name:
    return -ERR_INVALID_OBJECT_NAME;
  case no_such_bucket:
    return -ERR_NO_SUCH_BUCKET;
  case method_not_allowed:
    return -ERR_METHOD_NOT_ALLOWED;
  case invalid_digest:
    return -ERR_INVALID_DIGEST;
  case bad_digest:
    return -ERR_BAD_DIGEST;
  case unresolvable_email:
    return -ERR_UNRESOLVABLE_EMAIL;
  case invalid_part:
    return -ERR_INVALID_PART;
  case invalid_part_order:
    return -ERR_INVALID_PART_ORDER;
  case no_such_upload:
    return -ERR_NO_SUCH_UPLOAD;
  case request_timeout:
    return -ERR_REQUEST_TIMEOUT;
  case length_required:
    return -ERR_LENGTH_REQUIRED;
  case request_time_skewed:
    return -ERR_REQUEST_TIME_SKEWED;
  case bucket_exists:
    return -ERR_BUCKET_EXISTS;
  case bad_url:
    return -ERR_BAD_URL;
  case precondition_failed:
    return -ERR_PRECONDITION_FAILED;
  case not_modified:
    return -ERR_NOT_MODIFIED;
  case invalid_utf8:
    return -ERR_INVALID_UTF8;
  case unprocessable_entity:
    return -ERR_UNPROCESSABLE_ENTITY;
  case too_large:
    return -ERR_TOO_LARGE;
  case too_many_buckets:
    return -ERR_TOO_MANY_BUCKETS;
  case invalid_request:
    return -ERR_INVALID_REQUEST;
  case too_small:
    return -ERR_TOO_SMALL;
  case not_found:
    return -ERR_NOT_FOUND;
  case permanent_redirect:
    return -ERR_PERMANENT_REDIRECT;
  case locked:
    return -ERR_LOCKED;
  case quota_exceeded:
    return -ERR_QUOTA_EXCEEDED;
  case signature_no_match:
    return -ERR_SIGNATURE_NO_MATCH;
  case invalid_access_key:
    return -ERR_INVALID_ACCESS_KEY;
  case malformed_xml:
    return -ERR_MALFORMED_XML;
  case user_exist:
    return -ERR_USER_EXIST;
  case not_slo_manifest:
    return -ERR_NOT_SLO_MANIFEST;
  case email_exist:
    return -ERR_EMAIL_EXIST;
  case key_exist:
    return -ERR_KEY_EXIST;
  case invalid_secret_key:
    return -ERR_INVALID_SECRET_KEY;
  case invalid_key_type:
    return -ERR_INVALID_KEY_TYPE;
  case invalid_cap:
    return -ERR_INVALID_CAP;
  case invalid_tenant_name:
    return -ERR_INVALID_TENANT_NAME;
  case website_redirect:
    return -ERR_WEBSITE_REDIRECT;
  case no_such_website_configuration:
    return -ERR_NO_SUCH_WEBSITE_CONFIGURATION;
  case amz_content_sha256_mismatch:
    return -ERR_AMZ_CONTENT_SHA256_MISMATCH;
  case no_such_lc:
    return -ERR_NO_SUCH_LC;
  case no_such_user:
    return -ERR_NO_SUCH_USER;
  case no_such_subuser:
    return -ERR_NO_SUCH_SUBUSER;
  case mfa_required:
    return -ERR_MFA_REQUIRED;
  case no_such_cors_configuration:
    return -ERR_NO_SUCH_CORS_CONFIGURATION;
  case user_suspended:
    return -ERR_USER_SUSPENDED;
  case internal_error:
    return -ERR_INTERNAL_ERROR;
  case not_implemented:
    return -ERR_NOT_IMPLEMENTED;
  case service_unavailable:
    return -ERR_SERVICE_UNAVAILABLE;
  case role_exists:
    return -ERR_ROLE_EXISTS;
  case malformed_doc:
    return -ERR_MALFORMED_DOC;
  case no_role_found:
    return -ERR_NO_ROLE_FOUND;
  case delete_conflict:
    return -ERR_DELETE_CONFLICT;
  case no_such_bucket_policy:
    return -ERR_NO_SUCH_BUCKET_POLICY;
  case invalid_location_constraint:
    return -ERR_INVALID_LOCATION_CONSTRAINT;
  case tag_conflict:
    return -ERR_TAG_CONFLICT;
  case invalid_tag:
    return -ERR_INVALID_TAG;
  case zero_in_url:
    return -ERR_ZERO_IN_URL;
  case malformed_acl_error:
    return -ERR_MALFORMED_ACL_ERROR;
  case zonegroup_default_placement_misconfiguration:
    return -ERR_ZONEGROUP_DEFAULT_PLACEMENT_MISCONFIGURATION;
  case invalid_encryption_algorithm:
    return -ERR_INVALID_ENCRYPTION_ALGORITHM;
  case invalid_cors_rules_error:
    return -ERR_INVALID_CORS_RULES_ERROR;
  case no_cors_found:
    return -ERR_NO_CORS_FOUND;
  case invalid_website_routing_rules_error:
    return -ERR_INVALID_WEBSITE_ROUTING_RULES_ERROR;
  case rate_limited:
    return -ERR_RATE_LIMITED;
  case position_not_equal_to_length:
    return -ERR_POSITION_NOT_EQUAL_TO_LENGTH;
  case object_not_appendable:
    return -ERR_OBJECT_NOT_APPENDABLE;
  case invalid_bucket_state:
    return -ERR_INVALID_BUCKET_STATE;
  case busy_resharding:
    return -ERR_BUSY_RESHARDING;
  case no_such_entity:
    return -ERR_NO_SUCH_ENTITY;
  case packed_policy_too_large:
    return -ERR_PACKED_POLICY_TOO_LARGE;
  case invalid_identity_token:
    return -ERR_INVALID_IDENTITY_TOKEN;
  case user_not_permitted_to_use_placement_rule:
    return -EPERM;
  case requirement_exceeds_limit:
    return -EDEADLK;
  case placement_pool_missing:
    return -EIO;
  }
  return -EDOM;
}

const boost::system::error_category& rgw_category() noexcept {
  static const rgw_error_category c;
  return c;
}

#endif // RGW_ERROR_CODE_H
