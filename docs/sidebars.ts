import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  tutorialSidebar: [
    // Auto-generated content from existing docs
    'index',
    'API-Doc',
    'database-doc',
    'design-doc',
    
    // Manually configured Projects section
    {
      type: 'category',
      label: 'Projects',
      collapsed: true,
      items: [
        'projects/indexing',
        'projects/join-algorithms',
        'projects/buffer-manager',
        'projects/catalog-manager',
        'projects/sorted-ordered-file-manager',
        'projects/fsm-heap-manager',
        
        // First subsection: Fixed Length Data Types
        {
          type: 'category',
          label: 'Fixed Length Data Types',
          collapsed: true,
          items: [
            'projects/fixed-length-data-types/fixed-length-data-types',
          ],
        },
        // Second subsection: Variable Length Data Types
        {
          type: 'category',
          label: 'Variable Length Data Types',
          collapsed: true,
          items: [
            'projects/variable-length/varchar-text',
            'projects/variable-length/blob-array',
            'projects/variable-length/semi-structured',
          ],
        },
        // Select Project Aggregate
        {
          type: 'category',
          label: 'Select Project Aggregate',
          collapsed: true,
          items: [
            'projects/select-project-aggregate/selection',
            'projects/select-project-aggregate/projection',
            'projects/select-project-aggregate/aggregatation',
          ],
        },
        'projects/update-delete',
      ],
    },
  ],
};

export default sidebars;