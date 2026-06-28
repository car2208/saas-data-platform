from pathlib import Path

import yaml
from omegaconf import DictConfig, OmegaConf


def load_config(env: str = "dev") -> DictConfig:
    config_dir = Path("config")
    base_config = OmegaConf.load(config_dir / "base.yaml")
    env_config = OmegaConf.load(config_dir / "env" / f"{env}.yaml")
    return OmegaConf.merge(base_config, env_config)


def validate_config(config: DictConfig) -> None:
    required_fields = [
        "execution.start_date",
        "execution.end_date",
        "execution.tenant",
        "paths.raw",
        "paths.bronze",
        "paths.silver",
        "paths.gold",
        "paths.quarantine_root",
        "paths.quality_logs",
        "quality.fail_on_critical",
    ]
    for field in required_fields:
        val = OmegaConf.select(config, field)
        if val is None:
            raise ValueError(f"Campo requerido faltante: {field}")


def load_tenant_config(tenant_id: str) -> DictConfig:
    tenant_file = Path("config") / "tenants" / f"{tenant_id}.yaml"
    if tenant_file.exists():
        return OmegaConf.load(tenant_file)
    return OmegaConf.create({})


def get_available_tenants() -> list[str]:
    tenants_dir = Path("config/tenants")
    tenants = []
    if not tenants_dir.exists():
        return tenants
    for yaml_file in sorted(tenants_dir.glob("*.yaml")):
        with open(yaml_file) as f:
            tenant_config = yaml.safe_load(f)
            if tenant_config and "tenant_id" in tenant_config:
                tenants.append(tenant_config["tenant_id"])
    return tenants


def resolve_tenants_to_process(tenant_param: str) -> list[str]:
    if tenant_param == "all":
        return get_available_tenants()
    return [tenant_param.lower()]
