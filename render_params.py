from typing import Optional

import json5
from pydantic import BaseModel, ConfigDict


class AdobeAfterEffectsSettings(BaseModel):
    model_config = ConfigDict(extra='forbid')
    major_version: str
    aerender_dir: str
    image_cache_percent: Optional[int] = 50
    max_mem_percent: Optional[int] = 50
    close: Optional[str] = "PROMPT_TO_SAVE_CHANGES"
    play_sound: Optional[bool] = True
    multi_machine_settings: Optional[str] = ""
    mfr: Optional[bool] = True
    mfr_max_cpu_percent: Optional[int] = 100
    render_settings_template: str
    output_module_template: str
    delete_output_on_failure: bool


class HandbrakeSettings(BaseModel):
    model_config = ConfigDict(extra='forbid')
    bitrate: Optional[int] = 60000
    audio_bitrate: Optional[int] = 320
    audio_sample_rate: Optional[str] = "auto"
    encoder: Optional[str] = "nvenc_h265"
    encoder_preset: Optional[str] = "quality"
    hb_dir: str
    delete_intermediate_on_success: bool


class RenderParams(BaseModel):
    model_config = ConfigDict(extra='forbid')
    ae: AdobeAfterEffectsSettings
    hb: HandbrakeSettings

    @classmethod
    def from_json5(cls, file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json5.load(file)
            return cls(**data)
