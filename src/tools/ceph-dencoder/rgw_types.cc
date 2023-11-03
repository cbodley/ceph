#include "acconfig.h"
#include <cstdint>
using namespace std;
#include "include/ceph_features.h"

#define TYPE(t)
#define TYPE_STRAYDATA(t)
#define TYPE_NONDETERMINISTIC(t)
#define TYPE_FEATUREFUL(t)
#define TYPE_FEATUREFUL_STRAYDATA(t)
#define TYPE_FEATUREFUL_NONDETERMINISTIC(t)
#define TYPE_FEATUREFUL_NOCOPY(t)
#define TYPE_NOCOPY(t)
#define MESSAGE(t)
#include "rgw_types.h"
#undef TYPE
#undef TYPE_STRAYDATA
#undef TYPE_NONDETERMINISTIC
#undef TYPE_NOCOPY
#undef TYPE_FEATUREFUL
#undef TYPE_FEATUREFUL_STRAYDATA
#undef TYPE_FEATUREFUL_NONDETERMINISTIC
#undef TYPE_FEATUREFUL_NOCOPY
#undef MESSAGE

#include "denc_plugin.h"

void rgw_owner_wrapper::dump(Formatter* f) const
{
  encode_json("owner", *this, f);
}

void rgw_owner_wrapper::decode_json(JSONObj* obj)
{
  JSONDecoder::decode_json("owner", *this, obj);
}

void rgw_owner_wrapper::generate_test_instances(std::list<rgw_owner_wrapper*>& o)
{
  // user
  o.push_back(new rgw_owner_wrapper{rgw_user{"tenant", "userid"}});
  // role
  o.push_back(new rgw_owner_wrapper{rgw_user{"tenant", "roleid", "oidc"}});
  // account id
  o.push_back(new rgw_owner_wrapper{rgw_account_id{"RGW12345678901234567"}});
}

DENC_API void register_dencoders(DencoderPlugin* plugin)
{
#include "rgw_types.h"
}

DENC_API void unregister_dencoders(DencoderPlugin* plugin)
{
  plugin->unregister_dencoders();
}
