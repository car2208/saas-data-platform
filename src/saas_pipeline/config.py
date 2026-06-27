"""Módulo de configuración con OmegaConf"""

from pathlib import Path
from omegaconf import OmegaConf, DictConfig


def load_config(env: str = "dev") -> DictConfig:
    """
    Carga configuración jerárquica desde YAML.    
    Args:
        env: Ambiente (dev, qa, main)    
    Returns:
        DictConfig con la configuración cargada y mergeada
    """
    config_dir = Path("config")
    
    # Cargar base
    base_config = OmegaConf.load(config_dir / "base.yaml")
    
    # Cargar ambiente específico y mergear
    env_config = OmegaConf.load(config_dir / "env" / f"{env}.yaml")
    config = OmegaConf.merge(base_config, env_config)
    
    return config


def validate_config(config: DictConfig) -> None:
    """
    Valida que la configuración tenga los campos requeridos.soasasadsadsmadmadsmadsmadsmadsmadsmadsm,adsmadsmadsmadsmadsmadsmm
    
    Args:
        config: Configuración a validar
        
    Raises:
        ValueError: Si falta algún campo requerido
    """
    required_fields = [
        "execution.start_date",
        "execution.end_date",
        "execution.tenant",
        "paths.bronze",
        "paths.silver",
        "paths.gold",
        "quality.fail_on_critical",
    ]
    
    for field in required_fields:
        try:
            OmegaConf.select(config, field)
        except Exception as e:
            raise ValueError(f"Campo requerido faltante: {field}") from e



def get_available_tenants() -> list:
    """Lee los tenants disponibles desde config/tenants/"""
    from pathlib import Path
    import yaml
    
    tenants_dir = Path("config/tenants")
    tenants = []
    
    if not tenants_dir.exists():
        return tenants
    
    for yaml_file in sorted(tenants_dir.glob("*.yaml")):
        try:
            with open(yaml_file) as f:
                tenant_config = yaml.safe_load(f)
                if tenant_config and "tenant_id" in tenant_config:
                    tenants.append(tenant_config["tenant_id"])
        except Exception as e:
            print(f"Error leyendo {yaml_file}: {e}")
    
    return tenants


def resolve_tenants_to_process(tenant_param: str) -> list:
    """Resuelve cuáles tenants procesar"""
    if tenant_param == "all":
        return get_available_tenants()
    else:
        return [tenant_param.lower()]