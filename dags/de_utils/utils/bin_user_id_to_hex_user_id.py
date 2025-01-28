def bin_user_id_to_hex_user_id(binary_id):
    """Returns hex version of user_id from given binary_user_id
    Converts a binary user_id into hex user_id
    """
    # byte user_id are stored in big endian
    # thus we use big endian to convert bytes back to int
    # see: https://github.com/Enflick/textnow-mono/blob/18e52ddbe065363a3bcaebf90269bf8881d0ce72/lib/types/global_id.go
    long_id = int.from_bytes(binary_id, "big")

    # integer user_id is still being used by backend for internal comms,
    # however, for analytics purposes we want to stick to the norms and
    # use hex format user_id instead.

    # GUID logic in layman's terms:
    # The integer user_id is essentially a composition of multiple ids,
    # which can be extracted by right shifting the integer bits by a given
    # offset and boolean masking the shifted result.
    # guid is 62 bits long, so let's see the following example:
    # // Example (For absolute beginners):
    # The 4035506809862637934 has a binary representation of:
    #     11100000000001000000000001000000000010100010000100100101101110
    # The first 16 bits (from the LHS) 1110000000000100 are reserved for the envID and shardID.
    #  	- The shardID is the first 5 bits (from the RHS of 1110000000000100), which are 00100 and this equals to
    #  	      4 in decimal and hex.
    #  	- The envID is the following 10 bits (again, from RHS), which are 1100000000
    #  	- The last bit after the envID determines if the envID is reversed or not.
    # Since it is 1 this means the envID is reversed and so equals 0000000011, which is 3 in decimal and hex
    # The typeID is the 10 bits following the 16 bits of the envID and shardID and equals = 0000000001, which is
    #     1 in decimal and hex.
    # The localID represents the last 36 bits (from RHS) and so localID = 000000000010100010000100100101101110
    # see: https://github.com/Enflick/textnow-mono/blob/18e52ddbe065363a3bcaebf90269bf8881d0ce72/lib/types/global_id.go
    offset_flipped = 61
    offset_env = 51
    offset_shard = 46
    offset_type = 36
    offset_local = 0

    mask_flipped = 0x1
    mask_env = 0x3FF
    mask_shard = 0x1F
    mask_type = 0x3FF
    mask_local = 0xFFFFFFFFF

    # the only id that takes some extra logic is the environment id, this
    # requires us to do an extra check on whether the id should be reversed,
    # i.e. reverse the bits in the integer. The logic to check flip is the
    # same as any other ids.
    env_id = long_id >> offset_env & mask_env
    if long_id >> offset_flipped & mask_flipped == 1:
        env_id = int(f"{env_id << 6:10b}"[::-1], 2)
    shard_id = long_id >> offset_shard & mask_shard
    type_id = long_id >> offset_type & mask_type
    local_id = long_id >> offset_local & mask_local

    return f"{env_id:03x}-{shard_id:02x}-{type_id:03x}-{local_id:09x}"
