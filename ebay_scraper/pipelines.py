import json
import aiofiles
import pandas as pd
from itemadapter import ItemAdapter
from scrapy.utils.project import get_project_settings
import logging

class SavingPipeline:
    def __init__(self):
        self.output_format = get_project_settings().get('OUTPUT_FORMAT')
        self.chunk_size = get_project_settings().get('CHUNK_SIZE')
        self.current_chunk = []
        self.file_counter = 0

    def close_spider(self, spider):
        if self.current_chunk:
            spider.loop.run_until_complete(self.save_chunk())

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        self.current_chunk.append(adapter.asdict())
        if len(self.current_chunk) >= self.chunk_size:
            spider.loop.run_until_complete(self.save_chunk())
        return item

    async def save_chunk(self):
        self.file_counter += 1
        file_name = f'output_{self.file_counter}.{self.output_format}'
        
        if self.output_format == 'json':
            async with aiofiles.open(file_name, 'w', encoding='utf-8') as f:
                json_data = json.dumps(self.current_chunk, ensure_ascii=False, indent=4)
                lines = json_data.split('\n')
                indented_lines = ['    ' + line for line in lines]
                indented_json_data = '\n'.join(indented_lines)
                await f.write(indented_json_data)
        elif self.output_format == 'jsonlines':
            async with aiofiles.open(file_name, 'w', encoding='utf-8') as f:
                for item in self.current_chunk:
                    indented_line = '    ' + json.dumps(item, ensure_ascii=False).replace('\n', '\n    ')
                    await f.write(indented_line + '\n')
        elif self.output_format == 'parquet':
            df = pd.DataFrame(self.current_chunk)
            df.to_parquet(file_name)
        
        self.current_chunk = []
