from html_cleaner import clean_html
from datetime import datetime

EVM_NETWORKS_MAP = {
    "ethereum": 1,
    "optimism": 10,
    "polygon": 137,
    "celo": 42220,
    "base": 8453,
    "arbitrum": 42161,
    "gnosis": 100,
    "zkevm": 1101,
    "ethereum_classic": 61,
}

NON_EVM_NETWORKS_MAP = {"solana": 101, "stellar": 1500}

SOCIALS_MAP = {
    "FACEBOOK": "facebook",
    "X": "x",
    "INSTAGRAM": "instagram",
    "YOUTUBE": "youtube",
    "LINKEDIN": "linkedin",
    "REDDIT": "reddit",
    "DISCORD": "discord",
    "FARCASTER": "farcaster",
    "LENS": "lens",
    "WEBSITE": "website",
    "TELEGRAM": "telegram",
    "GITHUB": "github",
}


def extract_flat_project_data(project_data):
    addresses = (
        project_data[10]
        if len(project_data) > 10 and isinstance(project_data[10], dict)
        else {}
    )
    socials = (
        project_data[11]
        if len(project_data) > 11 and isinstance(project_data[11], dict)
        else {}
    )

    extracted_addresses = {
        f"{network_name}_address": addresses.get(network_type.upper(), {}).get(
            str(chain_id), None
        )
        for network_type, network_map in {
            "EVM": EVM_NETWORKS_MAP,
            **NON_EVM_NETWORKS_MAP,
        }.items()
        for network_name, chain_id in (
            network_map.items()
            if isinstance(network_map, dict)
            else [(network_type, network_map)]
        )
    }

    extracted_social_links = {
        SOCIALS_MAP[social_type]: socials.get(social_type, None)
        for social_type in SOCIALS_MAP.keys()
    }

    return {
        "id": project_data[0],
        "title": project_data[1],
        "description": clean_html(project_data[2]) if project_data[2] else None,
        "raised_amount": float(project_data[3]) if project_data[3] else None,
        "givbacks_eligible": (
            bool(project_data[4]) if project_data[4] is not None else None
        ),
        "listed": bool(project_data[5]) if project_data[5] is not None else None,
        "unique_donors": int(project_data[6]) if project_data[6] else None,
        "updated_at": (
            project_data[7].isoformat()
            if isinstance(project_data[7], datetime)
            else None
        ),
        "owner_wallet": project_data[8] if project_data[8] else None,
        "in_active_qf_round": (
            bool(project_data[9]) if project_data[9] is not None else None
        ),
        **extracted_addresses,
        **extracted_social_links,
        "giv_power": float(project_data[12]) if project_data[12] else None,
        "giv_power_rank": int(project_data[13]) if project_data[13] else None,
    }
