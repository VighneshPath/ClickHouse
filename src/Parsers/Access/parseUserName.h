#pragma once

#include <Core/Types.h>
#include <Parsers/IParser.h>


namespace DB
{
/// Parses a comma-separated list of user names. Each can be a simple string or identifier or
/// something like `name@host`, where `host` (an ip address, ip subnet, or host name, with the %
/// and _ wildcards as in LIKE) restricts where the user may connect from.
/// Supports query parameters if 'allow_query_parameter' is true, but not in the 'host' part.
bool parseUserNames(IParser::Pos & pos, Expected & expected, Strings & user_names, bool allow_query_parameter);

/// Parses either the 'CURRENT_USER' keyword (or some of its aliases).
bool parseCurrentUserTag(IParser::Pos & pos, Expected & expected);


/// Parses a comma-separated list of role names.
inline bool parseRoleNames(IParser::Pos & pos, Expected & expected, Strings & role_names)
{
    return parseUserNames(pos, expected, role_names, /*allow_query_parameter=*/ false);
}

}
