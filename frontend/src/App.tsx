import { Routes, Route } from 'react-router-dom'
import { Layout } from './components/Layout'
import { Dashboard } from './components/dashboard/Dashboard'
import { SchemaList } from './components/schemas/SchemaList'
import { SchemaEditor } from './components/schemas/SchemaEditor'
import { TaskList } from './components/tasks/TaskList'
import { TaskDetail } from './components/tasks/TaskDetail'
import { AIGenerator } from './components/schemas/AIGenerator'

function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<Dashboard />} />
        <Route path="/schemas" element={<SchemaList />} />
        <Route path="/schemas/new" element={<SchemaEditor />} />
        <Route path="/schemas/:schemaId" element={<SchemaEditor />} />
        <Route path="/schemas/generate" element={<AIGenerator />} />
        <Route path="/tasks" element={<TaskList />} />
        <Route path="/tasks/:taskId" element={<TaskDetail />} />
      </Routes>
    </Layout>
  )
}

export default App
